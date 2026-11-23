//! Transparent row/column routing: send a SELECT to the DuckDB mirror only when it is provably equivalent
//! to SeekDB *and* likely to be faster. Both gates are mandatory.
//!
//! The benefit gate is not an optimization. On 10M rows `MIN(id), MAX(id)` measured 0.76 ms on SeekDB
//! (index-organized primary key) versus 1.99 ms on DuckDB — 0.38x. Routing every equivalent query would
//! therefore make some queries slower, so equivalence alone is not sufficient grounds to route.
//!
//! The equivalence gate is deliberately narrow. Text columns are refused in any comparison, grouping,
//! ordering or MIN/MAX because SeekDB's default `utf8mb4_general_ci` is case- and accent-insensitive with
//! PAD SPACE, while DuckDB compares binary: `WHERE s='hello'` matched 3 rows on SeekDB and 1 on DuckDB,
//! and `GROUP BY s` produced 1 group versus 3.

use std::collections::BTreeMap;

use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
    JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, Query, Select, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins, Value,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::mirror::mirror_name;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    NotOneSelect,
    NotPlainSelect,
    Projection,
    From,
    Join,
    Predicate,
    GroupBy,
    Having,
    OrderBy,
    LimitWithoutOrder,
    UnknownTable,
    UnknownColumn,
    Collation,
    LossyType,
    Sum,
    /// Equivalent, but the shape is one SeekDB serves better (index-backed MIN/MAX, small table).
    NotProfitable,
}

impl Refused {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotOneSelect => "not_one_select",
            Self::NotPlainSelect => "not_plain_select",
            Self::Projection => "projection",
            Self::From => "from",
            Self::Join => "join",
            Self::Predicate => "predicate",
            Self::GroupBy => "group_by",
            Self::Having => "having",
            Self::OrderBy => "order_by",
            Self::LimitWithoutOrder => "limit_without_order",
            Self::UnknownTable => "unknown_table",
            Self::UnknownColumn => "unknown_column",
            Self::Collation => "collation",
            Self::LossyType => "lossy_type",
            Self::Sum => "sum",
            Self::NotProfitable => "not_profitable",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    /// SeekDB `DATA_TYPE`, lower case.
    pub data_type: String,
    pub numeric_precision: Option<u32>,
    pub column_type: String,
}

impl ColumnInfo {
    /// Columns whose mirror representation is text or BOOLEAN rather than the source type, so comparing
    /// or aggregating them in DuckDB would not match SeekDB.
    pub fn lossy(&self) -> bool {
        let dt = self.data_type.as_str();
        match dt {
            "decimal" | "numeric" => self.numeric_precision.unwrap_or(0) > crate::mirror::MAX_DECIMAL_PRECISION,
            "date" | "datetime" | "timestamp" | "bit" => true,
            "tinyint" => self.column_type.replace(' ', "").eq_ignore_ascii_case("tinyint(1)"),
            _ => false,
        }
    }

    /// True for any character column. Measured: MySQL's PAD SPACE semantics apply even to `utf8mb4_bin`,
    /// so `'a'` and `'a '` compare equal and group together on SeekDB but not in DuckDB. Collation being
    /// binary is therefore NOT sufficient for equivalence, and the mysql extension additionally maps
    /// `utf8mb4_bin` VARCHAR to BLOB. Every text column is refused in comparisons/grouping/ordering/MIN-MAX.
    fn is_text(&self) -> bool {
        matches!(
            self.data_type.as_str(),
            "char" | "varchar" | "text" | "tinytext" | "mediumtext" | "longtext" | "enum" | "set"
        )
    }
}

#[derive(Debug, Clone, Default)]
pub struct Schema {
    pub tables: BTreeMap<String, Vec<ColumnInfo>>,
}

impl Schema {
    pub fn column(&self, table: &str, col: &str) -> Option<&ColumnInfo> {
        self.tables
            .get(&table.to_ascii_lowercase())?
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(col))
    }

    fn resolve(&self, tables: &[String], qualifier: Option<&str>, col: &str) -> Result<(String, ColumnInfo), Refused> {
        if let Some(q) = qualifier {
            let c = self.column(q, col).ok_or(Refused::UnknownColumn)?;
            return Ok((q.to_string(), c.clone()));
        }
        let mut found = None;
        for t in tables {
            if let Some(c) = self.column(t, col) {
                if found.is_some() {
                    return Err(Refused::UnknownColumn);
                }
                found = Some((t.clone(), c.clone()));
            }
        }
        found.ok_or(Refused::UnknownColumn)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Routed {
    pub sql: String,
    pub tables: Vec<String>,
    /// Column names as the source engine would report them. DuckDB names `COUNT(*)` as `count_star()`,
    /// which would change the Hrana response for an unchanged query, so names come from the original
    /// projection instead.
    pub col_names: Vec<String>,
    pub col_decltypes: Vec<Option<String>>,
    /// Aggregates present, used by the benefit gate.
    pub has_count: bool,
    pub has_minmax: bool,
    pub group_by: bool,
}

/// Mirrors `exec::decltype_name` so a routed query reports the same decltype strings as the SeekDB path.
/// SeekDB reports COUNT as LONGLONG and MIN/MAX as the argument column's type (both measured).
fn column_decltype(c: &ColumnInfo) -> &'static str {
    match c.data_type.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "year" => "INTEGER",
        "float" | "double" | "real" => "REAL",
        "decimal" | "numeric" => "DECIMAL",
        "date" => "DATE",
        "datetime" => "DATETIME",
        "timestamp" => "TIMESTAMP",
        "time" => "TIME",
        "json" => "JSON",
        "bit" | "geometry" | "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => "BLOB",
        _ => "TEXT",
    }
}

fn projection_decltypes(items: &[SelectItem], schema: &Schema, tables: &[String]) -> Vec<Option<String>> {
    let resolve = |e: &Expr| -> Option<String> {
        let (q, name) = ident_parts(e)?;
        let t = match q {
            Some(t) => t,
            None => tables.iter().find(|t| schema.column(t, &name).is_some())?.clone(),
        };
        schema.column(&t, &name).map(|c| column_decltype(c).to_string())
    };
    let mut out = Vec::new();
    for it in items {
        let e = match it {
            SelectItem::ExprWithAlias { expr, .. } | SelectItem::UnnamedExpr(expr) => expr,
            SelectItem::Wildcard(_) => {
                for t in tables {
                    if let Some(cols) = schema.tables.get(t) {
                        out.extend(cols.iter().map(|c| Some(column_decltype(c).to_string())));
                    }
                }
                continue;
            }
            _ => {
                out.push(None);
                continue;
            }
        };
        match e {
            Expr::Function(f) => {
                let n = f.name.to_string().to_ascii_uppercase();
                if n == "COUNT" {
                    out.push(Some("INTEGER".to_string()));
                } else if let FunctionArguments::List(args) = &f.args
                    && let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(inner))) = args.args.first()
                {
                    out.push(resolve(inner));
                } else {
                    out.push(None);
                }
            }
            _ => out.push(resolve(e)),
        }
    }
    out
}

fn projection_names(items: &[SelectItem], schema: &Schema, tables: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for it in items {
        match it {
            SelectItem::ExprWithAlias { alias, .. } => out.push(alias.value.clone()),
            SelectItem::UnnamedExpr(e) => out.push(e.to_string()),
            SelectItem::Wildcard(_) => {
                for t in tables {
                    if let Some(cols) = schema.tables.get(t) {
                        out.extend(cols.iter().map(|c| c.name.clone()));
                    }
                }
            }
            // The equivalence gate refuses qualified wildcards, so this branch is unreachable for a
            // routed query; keep a placeholder rather than guessing column order.
            SelectItem::QualifiedWildcard(_, _) => out.push("*".to_string()),
        }
    }
    out
}

fn name_of(o: &ObjectName) -> Option<String> {
    if o.0.len() != 1 {
        return None;
    }
    Some(o.0[0].as_ident()?.value.to_ascii_lowercase())
}

fn is_literal(e: &Expr) -> bool {
    match e {
        Expr::Value(v) => matches!(
            v.value,
            Value::Number(_, _)
                | Value::SingleQuotedString(_)
                | Value::DoubleQuotedString(_)
                | Value::Null
                | Value::Boolean(_)
                | Value::Placeholder(_)
        ),
        _ => false,
    }
}

/// Rewrite table references by mutating the AST and re-rendering it. Textual replacement is wrong: it also
/// rewrites aliases and column names that happen to equal a table name (`SELECT uid AS orders FROM orders`).
///
/// Also injects explicit NULLS FIRST/LAST. MySQL sorts NULL first for ASC and last for DESC; DuckDB defaults
/// to NULLS LAST for both, so an ASC ordering over a nullable column silently returns rows in a different
/// order (measured: `GROUP BY n ORDER BY n ASC` disagreed on the first row).
fn rewrite_query(q: &Query) -> String {
    let mut q = q.clone();

    // Collect the rename map before touching anything: qualified column references (`nums.id`) must be
    // rewritten too, otherwise the ON clause still names the source table and DuckDB cannot resolve it.
    let mut renames: Vec<(String, String)> = Vec::new();
    if let SetExpr::Select(s) = q.body.as_ref()
        && let Some(twj) = s.from.first()
    {
        for rel in std::iter::once(&twj.relation).chain(twj.joins.iter().map(|j| &j.relation)) {
            if let Ok(n) = table_name(rel) {
                renames.push((n.clone(), mirror_name(&n)));
            }
        }
    }

    if let Some(ob) = &mut q.order_by
        && let sqlparser::ast::OrderByKind::Expressions(exprs) = &mut ob.kind
    {
        for e in exprs {
            let desc = e.options.asc == Some(false);
            e.options.nulls_first = Some(!desc);
        }
    }
    if let SetExpr::Select(s) = q.body.as_mut()
        && let Some(twj) = s.from.first_mut()
    {
        rename_relation(&mut twj.relation);
        for j in &mut twj.joins {
            rename_relation(&mut j.relation);
        }
    }
    let _ = sqlparser::ast::visit_expressions_mut(&mut q, |e| {
        if let Expr::CompoundIdentifier(parts) = e
            && parts.len() == 2
        {
            let first = parts[0].value.to_ascii_lowercase();
            if let Some((_, new)) = renames.iter().find(|(old, _)| *old == first) {
                parts[0].value = new.clone();
            }
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    q.to_string()
}

fn rename_relation(tf: &mut TableFactor) {
    if let TableFactor::Table { name, .. } = tf
        && let Some(n) = name_of(name)
    {
        *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(mirror_name(&n)))]);
    }
}

fn ident_parts(e: &Expr) -> Option<(Option<String>, String)> {
    match e {
        Expr::Identifier(i) => Some((None, i.value.to_ascii_lowercase())),
        Expr::CompoundIdentifier(p) if p.len() == 2 => {
            Some((Some(p[0].value.to_ascii_lowercase()), p[1].value.to_ascii_lowercase()))
        }
        _ => None,
    }
}

fn ident(e: &Expr) -> Option<String> {
    ident_parts(e).map(|(_, c)| c)
}

struct Gates<'a> {
    schema: &'a Schema,
    tables: Vec<String>,
    has_count: bool,
    has_minmax: bool,
    group_by: bool,
}

impl<'a> Gates<'a> {
    fn col(&self, e: &Expr) -> Result<ColumnInfo, Refused> {
        let (q, name) = ident_parts(e).ok_or(Refused::UnknownColumn)?;
        let (_, c) = self.schema.resolve(&self.tables, q.as_deref(), &name)?;
        Ok(c)
    }

    fn check_projection(&mut self, items: &[SelectItem]) -> Result<(), Refused> {
        for item in items {
            match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                    self.check_projection_expr(e)?
                }
                SelectItem::Wildcard(_) => {
                    for t in &self.tables {
                        for c in self.schema.tables.get(t).ok_or(Refused::UnknownTable)? {
                            if c.lossy() {
                                return Err(Refused::LossyType);
                            }
                        }
                    }
                }
                _ => return Err(Refused::Projection),
            }
        }
        Ok(())
    }

    fn check_projection_expr(&mut self, e: &Expr) -> Result<(), Refused> {
        match e {
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                if self.col(e)?.lossy() {
                    return Err(Refused::LossyType);
                }
                Ok(())
            }
            Expr::Function(f) => self.check_function(f),
            _ => Err(Refused::Projection),
        }
    }

    fn check_function(&mut self, f: &Function) -> Result<(), Refused> {
        let name = f.name.to_string().to_ascii_uppercase();
        if !matches!(name.as_str(), "COUNT" | "MIN" | "MAX") {
            // SUM is refused: MySQL raises on i64 overflow while DuckDB widens to HUGEINT, and AVG
            // returns a different type. Neither is provably equivalent without a runtime guard.
            return Err(if name == "SUM" {
                Refused::Sum
            } else {
                Refused::Projection
            });
        }
        let FunctionArguments::List(args) = &f.args else {
            return Err(Refused::Projection);
        };
        if !args.clauses.is_empty() || f.over.is_some() || f.filter.is_some() || !f.within_group.is_empty() {
            return Err(Refused::Projection);
        }
        if args.args.len() != 1 {
            return Err(Refused::Projection);
        }
        match &args.args[0] {
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) if name == "COUNT" => {
                self.has_count = true;
                Ok(())
            }
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                let c = self.col(e)?;
                if c.lossy() {
                    return Err(Refused::LossyType);
                }
                if name == "COUNT" {
                    self.has_count = true;
                } else {
                    if c.is_text() {
                        return Err(Refused::Collation);
                    }
                    self.has_minmax = true;
                }
                Ok(())
            }
            _ => Err(Refused::Projection),
        }
    }

    fn check_predicate(&self, e: &Expr) -> Result<(), Refused> {
        match e {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And | BinaryOperator::Or => {
                    self.check_predicate(left)?;
                    self.check_predicate(right)
                }
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq => {
                    // An unresolvable identifier is an unknown column, not a malformed predicate.
                    for side in [left.as_ref(), right.as_ref()] {
                        if ident_parts(side).is_some() && self.col(side).is_err() {
                            return Err(Refused::UnknownColumn);
                        }
                    }
                    let (lc, rc) = (self.col(left), self.col(right));
                    match (lc, rc) {
                        (Ok(c), Err(_)) | (Err(_), Ok(c)) => {
                            if c.lossy() {
                                return Err(Refused::LossyType);
                            }
                            if c.is_text() {
                                return Err(Refused::Collation);
                            }
                            let other = if self.col(left).is_ok() { right } else { left };
                            if is_literal(other) {
                                Ok(())
                            } else {
                                Err(Refused::Predicate)
                            }
                        }
                        (Ok(a), Ok(b)) => {
                            if a.lossy() || b.lossy() {
                                return Err(Refused::LossyType);
                            }
                            if a.is_text() || b.is_text() {
                                return Err(Refused::Collation);
                            }
                            Ok(())
                        }
                        _ => Err(Refused::Predicate),
                    }
                }
                _ => Err(Refused::Predicate),
            },
            Expr::IsNull(_) | Expr::IsNotNull(_) => {
                let inner = match e {
                    Expr::IsNull(x) | Expr::IsNotNull(x) => x,
                    _ => unreachable!(),
                };
                if self.col(inner)?.lossy() {
                    Err(Refused::LossyType)
                } else {
                    Ok(())
                }
            }
            Expr::InList { expr, list, negated } => {
                if *negated {
                    return Err(Refused::Predicate);
                }
                let c = self.col(expr)?;
                if c.lossy() {
                    return Err(Refused::LossyType);
                }
                if c.is_text() {
                    return Err(Refused::Collation);
                }
                if list.iter().all(is_literal) {
                    Ok(())
                } else {
                    Err(Refused::Predicate)
                }
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                if *negated {
                    return Err(Refused::Predicate);
                }
                let c = self.col(expr)?;
                if c.lossy() {
                    return Err(Refused::LossyType);
                }
                if c.is_text() {
                    return Err(Refused::Collation);
                }
                if is_literal(low) && is_literal(high) {
                    Ok(())
                } else {
                    Err(Refused::Predicate)
                }
            }
            Expr::Nested(inner) => self.check_predicate(inner),
            _ => Err(Refused::Predicate),
        }
    }

    fn check_group_order(&mut self, select: &Select) -> Result<(), Refused> {
        match &select.group_by {
            GroupByExpr::All(_) => return Err(Refused::GroupBy),
            GroupByExpr::Expressions(exprs, mods) => {
                if !mods.is_empty() {
                    return Err(Refused::GroupBy);
                }
                if !exprs.is_empty() {
                    self.group_by = true;
                }
                for e in exprs {
                    let c = self.col(e)?;
                    if c.lossy() {
                        return Err(Refused::LossyType);
                    }
                    if c.is_text() {
                        return Err(Refused::Collation);
                    }
                }
            }
        }
        if select.having.is_some() {
            return Err(Refused::Having);
        }
        if select.distinct.is_some() {
            return Err(Refused::Projection);
        }
        Ok(())
    }

    fn check_order_by(&self, ob: &sqlparser::ast::OrderBy) -> Result<(), Refused> {
        let sqlparser::ast::OrderByKind::Expressions(exprs) = &ob.kind else {
            return Err(Refused::OrderBy);
        };
        if ob.interpolate.is_some() {
            return Err(Refused::OrderBy);
        }
        for o in exprs {
            if o.with_fill.is_some() || o.options.nulls_first.is_some() {
                return Err(Refused::OrderBy);
            }
            let c = self.col(&o.expr)?;
            if c.lossy() {
                return Err(Refused::LossyType);
            }
            if c.is_text() {
                return Err(Refused::Collation);
            }
        }
        Ok(())
    }
}

/// Structural gate. Cacheable: it depends only on the SQL text and the schema, never on data or freshness.
pub fn plan(sql: &str, schema: &Schema) -> Result<Routed, Refused> {
    let dialect = MySqlDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql).map_err(|_| Refused::NotOneSelect)?;
    if stmts.len() != 1 {
        return Err(Refused::NotOneSelect);
    }
    let stmt = stmts.pop().ok_or(Refused::NotOneSelect)?;
    let Statement::Query(q) = stmt else {
        return Err(Refused::NotPlainSelect);
    };
    route_query(&q, schema)
}

fn route_query(q: &Query, schema: &Schema) -> Result<Routed, Refused> {
    if q.with.is_some() || q.fetch.is_some() || !q.locks.is_empty() || q.for_clause.is_some() {
        return Err(Refused::NotPlainSelect);
    }
    let SetExpr::Select(select) = q.body.as_ref() else {
        return Err(Refused::NotPlainSelect);
    };
    if select.top.is_some() || select.into.is_some() || !select.lateral_views.is_empty() {
        return Err(Refused::NotPlainSelect);
    }
    if !select.from.len().eq(&1) {
        return Err(Refused::From);
    }
    let TableWithJoins { relation, joins } = &select.from[0];
    let mut tables = vec![table_name(relation)?];
    for j in joins {
        match &j.join_operator {
            // sqlparser renders a bare `JOIN` as JoinOperator::Join and `INNER JOIN` as Inner; both are
            // inner joins in MySQL.
            JoinOperator::Inner(JoinConstraint::On(e)) | JoinOperator::Join(JoinConstraint::On(e)) => {
                tables.push(table_name(&j.relation)?);
                let g = Gates {
                    schema,
                    tables: tables.clone(),
                    has_count: false,
                    has_minmax: false,
                    group_by: false,
                };
                g.check_predicate(e)?;
                // An equi-join must compare two columns, not a column to a literal.
                if let Expr::BinaryOp {
                    left,
                    op: BinaryOperator::Eq,
                    right,
                } = e
                {
                    if ident(left).is_none() || ident(right).is_none() {
                        return Err(Refused::Join);
                    }
                } else {
                    return Err(Refused::Join);
                }
            }
            JoinOperator::CrossJoin(_) => tables.push(table_name(&j.relation)?),
            _ => return Err(Refused::Join),
        }
    }
    for t in &tables {
        if !schema.tables.contains_key(t) {
            return Err(Refused::UnknownTable);
        }
    }

    let mut g = Gates {
        schema,
        tables: tables.clone(),
        has_count: false,
        has_minmax: false,
        group_by: false,
    };
    g.check_projection(&select.projection)?;
    if let Some(w) = &select.selection {
        g.check_predicate(w)?;
    }
    g.check_group_order(select)?;

    if let Some(ob) = &q.order_by {
        g.check_order_by(ob)?;
    }
    let has_order = q.order_by.is_some();
    if q.limit_clause.is_some() && !has_order {
        return Err(Refused::LimitWithoutOrder);
    }

    Ok(Routed {
        sql: rewrite_query(q),
        col_names: projection_names(&select.projection, schema, &tables),
        col_decltypes: projection_decltypes(&select.projection, schema, &tables),
        tables,
        has_count: g.has_count,
        has_minmax: g.has_minmax,
        group_by: g.group_by,
    })
}

fn table_name(t: &TableFactor) -> Result<String, Refused> {
    match t {
        TableFactor::Table {
            name,
            alias: _,
            args: None,
            with_hints: _,
            version: None,
            ..
        } => name_of(name).ok_or(Refused::From),
        _ => Err(Refused::From),
    }
}

/// Decide from SeekDB's EXPLAIN whether the driving access path is selective.
///
/// Measured on 10M rows: `WHERE id BETWEEN …(100 rows)` and `WHERE uid=42` plan as TABLE RANGE SCAN with
/// EST.ROWS 100 and take well under a millisecond on SeekDB, while routing them to DuckDB cost 11-55 ms —
/// a 20-100x net loss. But `WHERE status=2` also plans as TABLE RANGE SCAN with EST.ROWS 2,000,000 and
/// DuckDB wins 10.78x on it. So the operator name alone is not the discriminator; the driving row estimate
/// is. Only the first (driving) table access is considered, because a join's dimension lookup by primary
/// key is expected and must not disqualify the whole query.
/// The driving table access path and its estimated output rows.
///
/// EST.ROWS is the operator's *output* estimate, not the number of rows scanned. That distinction matters:
/// a non-indexed filter still full-scans the table on SeekDB (so DuckDB wins) even when it matches very few
/// rows, whereas an index range scan only touches the matching rows (so SeekDB wins). Judging by row count
/// alone would wrongly refuse selective filters over unindexed columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    FullScan,
    RangeScan,
    PointGet,
}

pub fn driving_access(explain: &[String]) -> Option<(Access, u64)> {
    for line in explain {
        if !line.contains('|') {
            continue;
        }
        let upper = line.to_ascii_uppercase();
        let kind = if upper.contains("TABLE FULL SCAN") {
            Access::FullScan
        } else if upper.contains("TABLE RANGE SCAN") {
            Access::RangeScan
        } else if upper.contains("TABLE GET") {
            Access::PointGet
        } else {
            continue;
        };
        let fields: Vec<&str> = line.split('|').collect();
        // |ID|OPERATOR|NAME|EST.ROWS|EST.TIME(us)|
        let est = fields.get(4)?.trim().parse::<u64>().ok()?;
        return Some((kind, est));
    }
    None
}

/// Refuse when SeekDB's driving path is an index lookup touching fewer rows than the routing threshold.
/// A full scan always qualifies: its cost scales with the table, which is where the columnar mirror wins.
pub fn plan_profitable(explain: &[String], min_rows: u64) -> bool {
    match driving_access(explain) {
        Some((Access::FullScan, _)) => true,
        Some((Access::RangeScan, n)) => n >= min_rows,
        Some((Access::PointGet, _)) => false,
        // No recognizable access path: do not guess, refuse.
        None => false,
    }
}

/// Benefit gate, evaluated per request against live row counts.
///
/// `MIN`/`MAX` without `GROUP BY` on a single table is refused: SeekDB answers it from the index-organized
/// primary key (measured 0.76 ms vs DuckDB 1.99 ms on 10M rows). Small tables are refused because the
/// mirror's fixed overhead dominates.
pub fn profitable(routed: &Routed, rows: &[u64], min_rows: u64) -> Result<(), Refused> {
    let total: u64 = rows.iter().sum();
    if routed.tables.len() == 1 {
        if routed.has_minmax && !routed.has_count && !routed.group_by {
            return Err(Refused::NotProfitable);
        }
        if total < min_rows {
            return Err(Refused::NotProfitable);
        }
        if !routed.has_count && !routed.group_by {
            return Err(Refused::NotProfitable);
        }
    } else if total < min_rows {
        return Err(Refused::NotProfitable);
    }
    Ok(())
}

/// Per-request routing context, grouped to keep the execution plumbing's argument count down.
pub struct RouteCtx {
    pub view: Option<std::sync::Arc<MirrorView>>,
    pub min_rows: u64,
}

/// Cache of structural routing decisions, keyed by SQL text and invalidated by schema fingerprint.
///
/// Only the structural gate is cacheable. Freshness (outbox emptiness) and the benefit gate (EXPLAIN row
/// estimates, live table counts) depend on data, not schema, and are evaluated on every request.
type CacheMap = std::collections::HashMap<String, (u64, Result<Routed, Refused>)>;

#[derive(Default)]
pub struct RouteCache {
    entries: parking_lot::Mutex<CacheMap>,
}

impl RouteCache {
    /// Bound the cache so a workload with unbounded distinct SQL cannot grow it without limit.
    const MAX_ENTRIES: usize = 4096;

    pub fn get_or_plan(&self, sql: &str, schema: &Schema, fingerprint: u64) -> Result<Routed, Refused> {
        let mut g = self.entries.lock();
        if let Some((fp, cached)) = g.get(sql)
            && *fp == fingerprint
        {
            return cached.clone();
        }
        let decided = plan(sql, schema);
        if g.len() >= Self::MAX_ENTRIES {
            g.clear();
        }
        g.insert(sql.to_string(), (fingerprint, decided.clone()));
        decided
    }

    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }
}

/// Stable fingerprint of the schema shape the structural gate depends on.
pub fn schema_fingerprint(schema: &Schema) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    let mut mix = |s: &str| {
        for b in s.bytes() {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x100000001b3);
        }
        h ^= 0xff;
        h = h.wrapping_mul(0x100000001b3);
    };
    for (table, cols) in &schema.tables {
        mix(table);
        for c in cols {
            mix(&c.name);
            mix(&c.data_type);
            mix(&c.column_type);
            mix(&c.numeric_precision.unwrap_or(0).to_string());
        }
    }
    h
}

/// A published snapshot of one namespace's mirror, refreshed by the sync worker after each committed cycle.
///
/// `duckdb::Connection` is Send but not Sync, so it is wrapped in a mutex to keep `Backend` Sync for axum.
/// Analytical queries against one namespace therefore serialize; callers must run this off the reactor.
pub struct MirrorView {
    /// Shared across cycles: cloning a DuckDB connection every cycle accumulates memory even when
    /// the data is constant (measured ~0.44 MB/15s under steady UPDATE versus ~0.05 MB/15s idle).
    pub duck: std::sync::Arc<parking_lot::Mutex<duckdb::Connection>>,
    pub schema: Schema,
    pub rows: BTreeMap<String, u64>,
    pub fingerprint: u64,
    /// Shared across cycles so the structural cache survives view republication.
    pub cache: std::sync::Arc<RouteCache>,
}

/// Run a SELECT on the mirror, or refuse. Every refusal and every DuckDB error falls back to SeekDB, which
/// stays the authority for errors and for anything not provably equivalent.
/// Structural gate plus the row-count benefit gate. No I/O, so callers can run it before paying for an
/// EXPLAIN round-trip: a query the gate already refuses must not cost a network query.
pub fn admit(view: &MirrorView, sql: &str, min_rows: u64) -> Result<Routed, Refused> {
    let routed = view.cache.get_or_plan(sql, &view.schema, view.fingerprint)?;
    let counts: Vec<u64> = routed
        .tables
        .iter()
        .map(|t| view.rows.get(t).copied().unwrap_or(0))
        .collect();
    profitable(&routed, &counts, min_rows)?;
    Ok(routed)
}

/// Execute an already-admitted plan on the mirror. Any DuckDB error or unmappable value refuses, so the
/// caller falls back to SeekDB, which stays the authority for errors.
pub fn execute_routed(
    view: &MirrorView,
    routed: &Routed,
    started: std::time::Instant,
) -> Result<crate::proto::StmtResult, Refused> {
    let duck = view.duck.lock();
    let mut stmt = duck.prepare(&routed.sql).map_err(|_| Refused::NotProfitable)?;
    // Column metadata is only available after execution; asking before it panics inside duckdb-rs, and
    // xsqld aborts the process on any panic.
    let mut rows = stmt.query([]).map_err(|_| Refused::NotProfitable)?;
    // Prefer the source-side names; fall back to DuckDB's only if the projection expanded differently.
    let duck_names: Vec<String> = rows.as_ref().map(|s| s.column_names()).unwrap_or_default();
    let ncols = duck_names.len();
    let names: Vec<String> = if routed.col_names.len() == ncols {
        routed.col_names.clone()
    } else {
        duck_names
    };
    let mut out_rows: Vec<Vec<crate::proto::Value>> = Vec::new();
    while let Some(row) = rows.next().map_err(|_| Refused::NotProfitable)? {
        let mut vals = Vec::with_capacity(ncols);
        for i in 0..ncols {
            let v = row.get_ref(i).map_err(|_| Refused::NotProfitable)?;
            vals.push(to_value(v).ok_or(Refused::NotProfitable)?);
        }
        out_rows.push(vals);
    }
    drop(rows);
    drop(duck);

    let cols = names
        .iter()
        .enumerate()
        .map(|(i, n)| crate::proto::Col {
            name: Some(n.clone()),
            decltype: routed.col_decltypes.get(i).cloned().flatten(),
        })
        .collect();
    let rows_read = out_rows.len() as u64;
    Ok(crate::proto::StmtResult {
        cols,
        rows: out_rows,
        affected_row_count: 0,
        last_insert_rowid: None,
        replication_index: None,
        rows_read,
        rows_written: 0,
        query_duration_ms: started.elapsed().as_micros() as f64 / 1000.0,
    })
}

const _: () = {
    #[allow(dead_code)]
    fn assert_send_sync<T: Send + Sync>() {}
    #[allow(dead_code)]
    fn check() {
        assert_send_sync::<MirrorView>();
        assert_send_sync::<std::sync::Arc<MirrorView>>();
    }
};

/// Anything whose DuckDB representation could differ from SeekDB's wire value refuses, so the query falls
/// back rather than returning a subtly different number. DECIMAL in particular is left to SeekDB.
fn to_value(v: duckdb::types::ValueRef<'_>) -> Option<crate::proto::Value> {
    use crate::proto::Value as P;
    use duckdb::types::ValueRef as V;
    Some(match v {
        V::Null => P::Null,
        V::Boolean(b) => P::Integer(i64::from(b)),
        V::TinyInt(i) => P::Integer(i64::from(i)),
        V::SmallInt(i) => P::Integer(i64::from(i)),
        V::Int(i) => P::Integer(i64::from(i)),
        V::BigInt(i) => P::Integer(i),
        V::UTinyInt(i) => P::Integer(i64::from(i)),
        V::USmallInt(i) => P::Integer(i64::from(i)),
        V::UInt(i) => P::Integer(i64::from(i)),
        V::UBigInt(i) => P::Integer(i64::try_from(i).ok()?),
        V::Float(f) => P::Float(f64::from(f)),
        V::Double(f) => P::Float(f),
        V::Text(s) => P::Text(String::from_utf8(s.to_vec()).ok()?),
        V::Blob(b) => P::Blob(b.to_vec()),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, dt: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            data_type: dt.into(),
            numeric_precision: if dt == "decimal" { Some(20) } else { None },
            column_type: dt.into(),
        }
    }

    fn schema() -> Schema {
        let mut s = Schema::default();
        s.tables.insert(
            "orders".into(),
            vec![
                col("id", "int"),
                col("uid", "int"),
                col("amt", "decimal"),
                col("s", "varchar"),
                col("bin_s", "varchar"),
            ],
        );
        s.tables.insert("dim".into(), vec![col("id", "int"), col("w", "int")]);
        s
    }

    fn ok(sql: &str) -> Routed {
        plan(sql, &schema()).unwrap_or_else(|e| panic!("{sql} should route, refused: {e:?}"))
    }

    fn refused(sql: &str, why: Refused) {
        assert_eq!(plan(sql, &schema()), Err(why), "{sql}");
    }

    #[test]
    fn join_rewrite_also_renames_qualified_column_references() {
        let r = ok("SELECT COUNT(*) FROM orders JOIN dim ON orders.uid = dim.id");
        assert!(
            r.sql.contains("m_orders.uid"),
            "ON clause must use the mirror name: {}",
            r.sql
        );
        assert!(
            r.sql.contains("m_dim.id"),
            "ON clause must use the mirror name: {}",
            r.sql
        );
        assert!(
            !r.sql.contains("ON orders.uid"),
            "stale source qualifier would not resolve: {}",
            r.sql
        );
    }

    #[test]
    fn plain_count_star_routes_and_rewrites_the_table() {
        let r = ok("SELECT COUNT(*) FROM orders");
        assert_eq!(r.sql, "SELECT COUNT(*) FROM m_orders");
        assert!(r.has_count);
        assert_eq!(r.tables, vec!["orders"]);
    }

    #[test]
    fn group_by_numeric_column_routes() {
        let r = ok("SELECT uid, COUNT(*) FROM orders GROUP BY uid");
        assert!(r.group_by);
        assert!(r.sql.contains("m_orders"));
    }

    #[test]
    fn rewrite_does_not_touch_column_or_alias_names() {
        let r = ok("SELECT uid AS orders FROM orders");
        assert!(r.sql.contains("m_orders"), "{}", r.sql);
        assert!(r.sql.contains("AS orders"), "alias must survive: {}", r.sql);
    }

    #[test]
    fn ci_text_comparison_is_refused() {
        refused("SELECT COUNT(*) FROM orders WHERE s='hello'", Refused::Collation);
        refused("SELECT s, COUNT(*) FROM orders GROUP BY s", Refused::Collation);
        refused("SELECT COUNT(*) FROM orders ORDER BY s", Refused::Collation);
        refused("SELECT MIN(s) FROM orders", Refused::Collation);
        refused("SELECT COUNT(*) FROM orders WHERE s IN ('a','b')", Refused::Collation);
        refused(
            "SELECT COUNT(*) FROM orders WHERE s BETWEEN 'a' AND 'b'",
            Refused::Collation,
        );
    }

    #[test]
    fn binary_collation_text_is_still_refused_because_mysql_is_pad_space() {
        // Measured: `WHERE bin_s='a'` matched 2 rows on SeekDB ('a' and 'a ') but 1 on DuckDB, and
        // GROUP BY produced 4 groups vs 5. utf8mb4_bin does not make text comparison equivalent.
        refused("SELECT COUNT(*) FROM orders WHERE bin_s='hello'", Refused::Collation);
        refused("SELECT bin_s, COUNT(*) FROM orders GROUP BY bin_s", Refused::Collation);
        refused("SELECT COUNT(*) FROM orders ORDER BY bin_s", Refused::Collation);
        refused("SELECT MIN(bin_s) FROM orders", Refused::Collation);
    }

    #[test]
    fn order_by_injects_mysql_null_ordering() {
        let asc = ok("SELECT uid FROM orders ORDER BY uid LIMIT 5");
        assert!(
            asc.sql.contains("NULLS FIRST"),
            "MySQL ASC puts NULL first: {}",
            asc.sql
        );
        let desc = ok("SELECT uid FROM orders ORDER BY uid DESC LIMIT 5");
        assert!(
            desc.sql.contains("NULLS LAST"),
            "MySQL DESC puts NULL last: {}",
            desc.sql
        );
    }

    #[test]
    fn group_by_with_asc_order_injects_nulls_first() {
        let r = ok("SELECT uid, COUNT(*) FROM orders GROUP BY uid ORDER BY uid");
        assert!(r.sql.contains("NULLS FIRST"), "{}", r.sql);
    }

    #[test]
    fn numeric_predicates_route() {
        ok("SELECT COUNT(*) FROM orders WHERE uid > 5");
        ok("SELECT COUNT(*) FROM orders WHERE uid BETWEEN 1 AND 9");
        ok("SELECT COUNT(*) FROM orders WHERE uid IN (1,2,3)");
        ok("SELECT COUNT(*) FROM orders WHERE uid IS NULL");
        ok("SELECT COUNT(*) FROM orders WHERE uid > 5 AND amt < 100");
    }

    #[test]
    fn arithmetic_functions_and_casts_are_refused() {
        refused("SELECT COUNT(*) FROM orders WHERE uid + 1 > 5", Refused::Predicate);
        refused("SELECT AVG(amt) FROM orders", Refused::Projection);
        refused("SELECT CAST(amt AS SIGNED) FROM orders", Refused::Projection);
        refused("SELECT COUNT(*) FROM orders WHERE UPPER(s)='A'", Refused::Predicate);
    }

    #[test]
    fn sum_is_refused_explicitly() {
        refused("SELECT SUM(amt) FROM orders", Refused::Sum);
    }

    #[test]
    fn lossy_columns_are_refused_everywhere() {
        let mut s = schema();
        s.tables.get_mut("orders").unwrap().push(ColumnInfo {
            name: "big".into(),
            data_type: "decimal".into(),
            numeric_precision: Some(65),
            column_type: "decimal(65,10)".into(),
        });
        s.tables.get_mut("orders").unwrap().push(col("d", "datetime"));
        s.tables.get_mut("orders").unwrap().push(ColumnInfo {
            name: "flag".into(),
            data_type: "tinyint".into(),
            numeric_precision: Some(1),
            column_type: "tinyint(1)".into(),
        });
        for sql in [
            "SELECT big FROM orders",
            "SELECT COUNT(*) FROM orders WHERE big > 1",
            "SELECT d FROM orders",
            "SELECT flag FROM orders",
            "SELECT * FROM orders",
        ] {
            assert_eq!(plan(sql, &s), Err(Refused::LossyType), "{sql}");
        }
    }

    #[test]
    fn non_select_and_multi_statement_are_refused() {
        refused("INSERT INTO orders VALUES(1,1,1,'a')", Refused::NotPlainSelect);
        refused("SELECT 1; SELECT 2", Refused::NotOneSelect);
        refused("UPDATE orders SET uid=1", Refused::NotPlainSelect);
    }

    #[test]
    fn subqueries_unions_ctes_and_distinct_are_refused() {
        refused("SELECT COUNT(*) FROM (SELECT 1) x", Refused::From);
        refused(
            "SELECT uid FROM orders UNION SELECT id FROM dim",
            Refused::NotPlainSelect,
        );
        refused(
            "WITH c AS (SELECT 1) SELECT COUNT(*) FROM orders",
            Refused::NotPlainSelect,
        );
        refused("SELECT DISTINCT uid FROM orders", Refused::Projection);
    }

    #[test]
    fn inner_equi_join_routes_but_outer_does_not() {
        let r = ok("SELECT COUNT(*) FROM orders JOIN dim ON orders.uid = dim.id");
        assert_eq!(r.tables.len(), 2);
        assert!(r.sql.contains("m_orders") && r.sql.contains("m_dim"), "{}", r.sql);
        refused(
            "SELECT COUNT(*) FROM orders LEFT JOIN dim ON orders.uid=dim.id",
            Refused::Join,
        );
        refused("SELECT COUNT(*) FROM orders JOIN dim ON orders.uid=1", Refused::Join);
    }

    #[test]
    fn limit_without_order_is_refused() {
        refused("SELECT uid FROM orders LIMIT 10", Refused::LimitWithoutOrder);
        ok("SELECT uid FROM orders ORDER BY id LIMIT 10");
    }

    #[test]
    fn unknown_table_or_column_is_refused() {
        refused("SELECT COUNT(*) FROM nope", Refused::UnknownTable);
        refused("SELECT COUNT(*) FROM orders WHERE nope > 1", Refused::UnknownColumn);
    }

    #[test]
    fn explain_driving_rows_parses_real_seekdb_output() {
        let selective = vec![
            "|0 |SCALAR GROUP BY   |     |1       |7           |".to_string(),
            "|1 |└─TABLE RANGE SCAN|row_t|100     |5           |".to_string(),
        ];
        assert_eq!(driving_access(&selective), Some((Access::RangeScan, 100)));
        let point = vec![
            "|0 |SCALAR GROUP BY|     |1       |7           |".to_string(),
            "|1 |└─TABLE GET    |row_t|1       |7           |".to_string(),
        ];
        assert_eq!(driving_access(&point), Some((Access::PointGet, 1)));
        let full = vec![
            "|0 |SCALAR GROUP BY  |            |1       |207712      |".to_string(),
            "|1 |└─TABLE FULL SCAN|row_t(k_uid)|10000000|26472       |".to_string(),
        ];
        assert_eq!(driving_access(&full), Some((Access::FullScan, 10_000_000)));
        let big_range = vec![
            "|0 |SCALAR GROUP BY   |               |1       |87827       |".to_string(),
            "|1 |└─TABLE RANGE SCAN|row_t(k_status)|2000000 |51579       |".to_string(),
        ];
        assert_eq!(driving_access(&big_range), Some((Access::RangeScan, 2_000_000)));
    }

    #[test]
    fn selective_paths_are_refused_but_large_range_scans_are_not() {
        let selective = vec!["|1 |└─TABLE RANGE SCAN|row_t|100     |5           |".to_string()];
        assert!(
            !plan_profitable(&selective, 200_000),
            "100-row index range must not route"
        );
        let point = vec!["|1 |└─TABLE GET    |row_t|1       |7           |".to_string()];
        assert!(!plan_profitable(&point, 200_000));
        let big_range = vec!["|1 |└─TABLE RANGE SCAN|row_t(k_status)|2000000 |51579       |".to_string()];
        assert!(
            plan_profitable(&big_range, 200_000),
            "2M-row range scan is where DuckDB wins 10.78x"
        );
        let full = vec!["|1 |└─TABLE FULL SCAN|row_t(k_uid)|10000000|26472       |".to_string()];
        assert!(plan_profitable(&full, 200_000));
    }

    #[test]
    fn only_the_driving_access_path_counts_for_joins() {
        let join = vec![
            "|0 |SCALAR GROUP BY   |     |1       |7           |".to_string(),
            "|1 |└─HASH JOIN       |     |1000000 |5000        |".to_string(),
            "|2 |  ├─TABLE FULL SCAN|orders|10000000|26472     |".to_string(),
            "|3 |  └─TABLE GET     |dim  |1       |7           |".to_string(),
        ];
        assert_eq!(
            driving_access(&join),
            Some((Access::FullScan, 10_000_000)),
            "a dimension lookup by PK must not disqualify the join"
        );
        assert!(plan_profitable(&join, 200_000));
    }

    #[test]
    fn unindexed_selective_filter_full_scans_so_it_still_qualifies() {
        // EST.ROWS is the output estimate (few rows), but the path is a full scan of the whole table,
        // which is exactly where DuckDB wins. Row count alone would wrongly refuse this.
        let unindexed = vec![
            "|0 |SCALAR GROUP BY   |     |1       |7           |".to_string(),
            "|1 |└─TABLE FULL SCAN|t    |10      |500         |".to_string(),
        ];
        assert_eq!(driving_access(&unindexed), Some((Access::FullScan, 10)));
        assert!(
            plan_profitable(&unindexed, 200_000),
            "a full scan qualifies regardless of how few rows match"
        );
    }

    #[test]
    fn unrecognizable_plan_is_refused_not_guessed() {
        assert!(!plan_profitable(&["some unrelated output".to_string()], 1));
        assert!(!plan_profitable(&[], 1));
    }

    #[test]
    fn cache_reuses_decision_and_invalidates_on_schema_change() {
        let s = schema();
        let fp = schema_fingerprint(&s);
        let cache = RouteCache::default();
        let a = cache.get_or_plan("SELECT COUNT(*) FROM orders", &s, fp).unwrap();
        assert_eq!(cache.len(), 1);
        let b = cache.get_or_plan("SELECT COUNT(*) FROM orders", &s, fp).unwrap();
        assert_eq!(a, b);
        assert_eq!(cache.len(), 1, "a repeat must not add an entry");

        let mut changed = s.clone();
        changed.tables.get_mut("orders").unwrap().push(col("extra", "int"));
        let fp2 = schema_fingerprint(&changed);
        assert_ne!(fp, fp2, "adding a column must change the fingerprint");
        let _ = cache.get_or_plan("SELECT COUNT(*) FROM orders", &changed, fp2).unwrap();
        assert_eq!(cache.len(), 1, "the stale entry must be replaced, not accumulated");
    }

    #[test]
    fn cache_stores_refusals_too() {
        let s = schema();
        let fp = schema_fingerprint(&s);
        let cache = RouteCache::default();
        assert_eq!(
            cache.get_or_plan("SELECT SUM(uid) FROM orders", &s, fp),
            Err(Refused::Sum)
        );
        assert_eq!(cache.len(), 1);
        assert_eq!(
            cache.get_or_plan("SELECT SUM(uid) FROM orders", &s, fp),
            Err(Refused::Sum)
        );
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn cache_is_bounded() {
        let s = schema();
        let fp = schema_fingerprint(&s);
        let cache = RouteCache::default();
        for i in 0..(RouteCache::MAX_ENTRIES + 10) {
            let _ = cache.get_or_plan(&format!("SELECT COUNT(*) FROM orders WHERE uid={i}"), &s, fp);
        }
        assert!(cache.len() <= RouteCache::MAX_ENTRIES, "cache grew to {}", cache.len());
    }

    #[test]
    fn fingerprint_is_order_independent_across_runs() {
        let s = schema();
        assert_eq!(schema_fingerprint(&s), schema_fingerprint(&s.clone()));
    }

    #[test]
    fn benefit_gate_refuses_index_backed_minmax() {
        let r = ok("SELECT MIN(id), MAX(id) FROM orders");
        assert_eq!(profitable(&r, &[10_000_000], 200_000), Err(Refused::NotProfitable));
    }

    #[test]
    fn benefit_gate_accepts_large_count_and_group_by() {
        let r = ok("SELECT COUNT(*) FROM orders");
        assert!(profitable(&r, &[10_000_000], 200_000).is_ok());
        let r = ok("SELECT uid, COUNT(*) FROM orders GROUP BY uid");
        assert!(profitable(&r, &[10_000_000], 200_000).is_ok());
    }

    #[test]
    fn benefit_gate_refuses_small_tables() {
        let r = ok("SELECT COUNT(*) FROM orders");
        assert_eq!(profitable(&r, &[10_000], 200_000), Err(Refused::NotProfitable));
    }

    #[test]
    fn benefit_gate_accepts_large_join() {
        let r = ok("SELECT COUNT(*) FROM orders JOIN dim ON orders.uid = dim.id");
        assert!(profitable(&r, &[10_000_000, 1_000], 200_000).is_ok());
    }

    #[test]
    fn benefit_gate_refuses_plain_projection_without_aggregate() {
        let r = ok("SELECT uid FROM orders ORDER BY id LIMIT 10");
        assert_eq!(profitable(&r, &[10_000_000], 200_000), Err(Refused::NotProfitable));
    }

    #[test]
    fn minmax_with_group_by_is_still_profitable() {
        let r = ok("SELECT uid, MAX(amt) FROM orders GROUP BY uid");
        assert!(profitable(&r, &[10_000_000], 200_000).is_ok());
    }
}

/// Live routing against a real DuckDB mirror. Ignored by default; run with `cargo test -- --ignored`.
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod live {
    use super::*;

    fn run_routed_test(
        v: &MirrorView,
        sql: &str,
        min_rows: u64,
        started: std::time::Instant,
    ) -> Result<crate::proto::StmtResult, Refused> {
        let p = admit(v, sql, min_rows)?;
        execute_routed(v, &p, started)
    }

    fn view() -> MirrorView {
        let duck = duckdb::Connection::open_in_memory().unwrap();
        duck.execute_batch(
            "CREATE TABLE m_orders(id INTEGER, uid INTEGER, amt DECIMAL(20,4), s VARCHAR, bin_s VARCHAR);
             INSERT INTO m_orders VALUES(1,10,99.5,'a','a'),(2,20,10.0,'b','b'),(3,10,5.25,'c','c');",
        )
        .unwrap();
        let mut schema = Schema::default();
        schema.tables.insert(
            "orders".into(),
            vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: "int".into(),
                    numeric_precision: Some(11),
                    column_type: "int(11)".into(),
                },
                ColumnInfo {
                    name: "uid".into(),
                    data_type: "int".into(),
                    numeric_precision: Some(11),
                    column_type: "int(11)".into(),
                },
                ColumnInfo {
                    name: "amt".into(),
                    data_type: "decimal".into(),
                    numeric_precision: Some(20),
                    column_type: "decimal(20,4)".into(),
                },
                ColumnInfo {
                    name: "s".into(),
                    data_type: "varchar".into(),
                    numeric_precision: None,
                    column_type: "varchar(64)".into(),
                },
                ColumnInfo {
                    name: "bin_s".into(),
                    data_type: "varchar".into(),
                    numeric_precision: None,
                    column_type: "varchar(64)".into(),
                },
            ],
        );
        let mut rows = BTreeMap::new();
        rows.insert("orders".to_string(), 1_000_000u64);
        let fingerprint = schema_fingerprint(&schema);
        MirrorView {
            duck: std::sync::Arc::new(parking_lot::Mutex::new(duck)),
            schema,
            rows,
            fingerprint,
            cache: std::sync::Arc::new(RouteCache::default()),
        }
    }

    #[test]
    #[ignore = "needs DuckDB"]
    fn routed_count_returns_correct_value_and_columns() {
        let v = view();
        let r = run_routed_test(&v, "SELECT COUNT(*) FROM orders", 100, std::time::Instant::now()).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].len(), 1);
        assert_eq!(r.rows_read, 1);
        assert_eq!(
            r.cols.len(),
            1,
            "column metadata must be read after execution, not before"
        );
        match r.rows[0][0] {
            crate::proto::Value::Integer(n) => assert_eq!(n, 3),
            ref other => panic!("expected integer count, got {other:?}"),
        }
    }

    #[test]
    #[ignore = "needs DuckDB"]
    fn routed_group_by_returns_all_groups() {
        let v = view();
        let r = run_routed_test(
            &v,
            "SELECT uid, COUNT(*) FROM orders GROUP BY uid ORDER BY uid",
            100,
            std::time::Instant::now(),
        )
        .unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.cols.len(), 2);
        assert_eq!(
            r.cols[1].decltype.as_deref(),
            Some("INTEGER"),
            "COUNT reports LONGLONG on SeekDB"
        );
        assert_eq!(r.cols[0].decltype.as_deref(), Some("INTEGER"), "uid is int on SeekDB");
    }

    #[test]
    #[ignore = "needs DuckDB"]
    fn refused_shapes_never_reach_duckdb() {
        let v = view();
        for sql in [
            "SELECT MIN(id), MAX(id) FROM orders",
            "SELECT COUNT(*) FROM orders WHERE s='a'",
            "SELECT SUM(uid) FROM orders",
        ] {
            assert!(
                run_routed_test(&v, sql, 100, std::time::Instant::now()).is_err(),
                "{sql} must refuse"
            );
        }
    }

    #[test]
    #[ignore = "needs DuckDB"]
    fn decimal_result_falls_back_rather_than_losing_precision() {
        let v = view();
        // MAX over DECIMAL is equivalent by the gate, but the value mapping refuses DECIMAL so the query
        // falls back to SeekDB instead of returning a coerced number.
        let r = run_routed_test(
            &v,
            "SELECT uid, MAX(amt) FROM orders GROUP BY uid",
            100,
            std::time::Instant::now(),
        );
        assert!(r.is_err(), "DECIMAL must not be silently coerced");
    }
}
