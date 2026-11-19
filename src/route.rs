//! Transparent row/column routing: decides whether one SELECT can be answered by the DuckDB mirror instead of
//! SQLite, and rewrites it for that engine.
//!
//! The rule is **only route when the two engines provably agree**. `tests/diff_engines.py` runs the same SQL on
//! both and compares values with their types; `tests/fixtures/engine-verdicts.json` is that evidence. Every
//! refusal below corresponds to a divergence measured there, not to a guess:
//!
//! - `CAST(AVG(v) AS BIGINT)` on an average of 1.5 gives 1 on SQLite and 2 on DuckDB → no CAST, no AVG, no arithmetic
//! - integer `/` gives 5 on SQLite and 5.0 on DuckDB → no arithmetic at all
//! - `MIN`/`MAX` over a column holding several storage classes orders them differently → uniform columns only
//! - `GROUP BY`/`ORDER BY` on a NOCASE or RTRIM column groups and sorts differently → BINARY only
//! - text that is not UTF-8 is stored as a blob in the mirror → such a column reads as mixed and is refused
//! - `ORDER BY x ASC` puts NULLs first on SQLite and last on DuckDB → the rewriter states NULLS explicitly
//!   (the harness confirms that stating them makes the two agree)
//! - `SUM` past i64 raises an overflow error on SQLite, while the mirror's VARIANT sum wraps silently to a
//!   negative number still inside i64 → no runtime check can catch it, so SUM is not routed at all
//!
//! Anything not on the allowed shape is refused. Being refused only means SQLite answers the query, which is
//! always correct; the cost of a wrong admission is a silently wrong answer.

use std::collections::BTreeMap;

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, JoinConstraint, JoinOperator, ObjectNamePart,
    OrderByKind, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

/// One column of a source table, as SQLite describes it
#[derive(Clone, Debug)]
pub struct ColumnInfo {
    pub name: String,
    pub declared: Option<String>,
    pub collation: String,
    pub primary_key: bool,
}

#[derive(Clone, Debug, Default)]
pub struct TableInfo {
    /// In `pragma_table_xinfo` order, which is the order the mirror's purity string uses
    pub columns: Vec<ColumnInfo>,
}

pub type Schema = BTreeMap<String, TableInfo>;

/// A query the mirror may answer, already rewritten for it
#[derive(Clone, Debug)]
pub struct Routed {
    pub sql: String,
    /// Source table names, lower case, in FROM order
    pub tables: Vec<String>,
    /// Declared type of each output column, in projection order. SQLite reports the source column's type; the
    /// mirror has none of its own, so the router supplies it. An aggregate has no declared type on either side.
    pub decltypes: Vec<Option<String>>,
    /// Every (table, column index) the query touches, so the caller can re-check storage-class purity live
    pub referenced: Vec<(String, usize)>,
}

/// Why SQLite has to answer this one. The text is a metric label, so it stays a fixed vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    AmbiguousColumn,
    Collation,
    /// SUM over the mirror's VARIANT columns can wrap silently inside i64 (measured: a sum past i64::MAX came
    /// back as a negative in-range number where SQLite raises an overflow error). No runtime type check can see
    /// that, so SUM is not routed at all until the mirror stores native integer columns.
    Sum,
}

impl Refused {
    pub fn label(self) -> &'static str {
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
            Self::AmbiguousColumn => "ambiguous_column",
            Self::Collation => "collation",
            Self::Sum => "sum",
        }
    }
}

/// A column's storage classes admit it when they are uniform apart from NULLs. `r` (real) and `b` (blob, which
/// is also where non-UTF-8 text lands) are never admitted: float accumulation order and blob ordering differ.
fn uniform(classes: &str) -> bool {
    let mut kinds: Vec<char> = classes.chars().filter(|c| *c != 'n').collect();
    kinds.sort_unstable();
    kinds.dedup();
    kinds.is_empty() || kinds == ['i'] || kinds == ['t']
}

/// The read-only inputs of one decision, plus the FROM aliases resolved so far
struct Scope<'a> {
    schema: &'a Schema,
    /// alias or table name (lower case) → source table name
    aliases: BTreeMap<String, String>,
    tables: Vec<String>,
    referenced: std::cell::RefCell<Vec<(String, usize)>>,
}

impl Scope<'_> {
    fn resolve(&self, parts: &[String]) -> Result<(String, usize, ColumnInfo), Refused> {
        match parts.len() {
            1 => {
                let name = parts[0].to_ascii_lowercase();
                let mut found: Option<(String, usize, ColumnInfo)> = None;
                for table in &self.tables {
                    if let Some((index, column)) = self.column_of(table, &name) {
                        if found.is_some() {
                            return Err(Refused::AmbiguousColumn);
                        }
                        found = Some((table.clone(), index, column));
                    }
                }
                found.ok_or(Refused::UnknownColumn)
            }
            2 => {
                let table = self
                    .aliases
                    .get(&parts[0].to_ascii_lowercase())
                    .ok_or(Refused::UnknownColumn)?;
                let (index, column) = self
                    .column_of(table, &parts[1].to_ascii_lowercase())
                    .ok_or(Refused::UnknownColumn)?;
                Ok((table.clone(), index, column))
            }
            _ => Err(Refused::UnknownColumn),
        }
    }

    fn column_of(&self, table: &str, column: &str) -> Option<(usize, ColumnInfo)> {
        self.schema
            .get(table)?
            .columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.name.eq_ignore_ascii_case(column))
            .map(|(i, c)| (i, c.clone()))
    }
}

/// A column reference that the mirror can reproduce exactly
fn check_column(parts: &[String], scope: &Scope) -> Result<(String, usize), Refused> {
    let (table, index, column) = scope.resolve(parts)?;
    if !column.collation.eq_ignore_ascii_case("BINARY") {
        return Err(Refused::Collation);
    }
    scope.referenced.borrow_mut().push((table.clone(), index));
    Ok((table, index))
}

/// Storage-class purity, checked against the live mirror on every request: a column that starts holding reals or
/// blobs must stop being routed even though the schema version did not change. An unrecorded column is refused
/// rather than assumed clean.
pub fn purity_allows(routed: &Routed, purity: &BTreeMap<String, Vec<String>>) -> bool {
    routed.referenced.iter().all(|(table, index)| {
        purity
            .get(table)
            .and_then(|cols| cols.get(*index))
            .is_some_and(|classes| !classes.is_empty() && uniform(classes))
    })
}

fn ident_parts(expr: &Expr) -> Option<Vec<String>> {
    match expr {
        Expr::Identifier(ident) => Some(vec![ident.value.clone()]),
        Expr::CompoundIdentifier(parts) => Some(parts.iter().map(|i| i.value.clone()).collect()),
        _ => None,
    }
}

/// A constant: a literal, a signed literal, or parentheses around one. Never a CAST or a function.
fn literal(expr: &Expr) -> bool {
    match expr {
        Expr::Value(_) => true,
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => literal(expr),
        _ => false,
    }
}

/// WHERE may only compare a column to a constant, so SQLite's affinity rules and DuckDB's agree
fn check_predicate(expr: &Expr, scope: &Scope) -> Result<(), Refused> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            use sqlparser::ast::BinaryOperator::*;
            match op {
                And | Or => {
                    check_predicate(left, scope)?;
                    check_predicate(right, scope)
                }
                Eq | NotEq | Lt | LtEq | Gt | GtEq => {
                    let l = ident_parts(left);
                    let r = ident_parts(right);
                    match (l, r) {
                        (Some(parts), None) if literal(right) => {
                            check_column(&parts, scope)?;
                            Ok(())
                        }
                        (None, Some(parts)) if literal(left) => {
                            check_column(&parts, scope)?;
                            Ok(())
                        }
                        _ => Err(Refused::Predicate),
                    }
                }
                _ => Err(Refused::Predicate),
            }
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            let parts = ident_parts(inner).ok_or(Refused::Predicate)?;
            check_column(&parts, scope)?;
            Ok(())
        }
        Expr::Between { expr, low, high, .. } => {
            let parts = ident_parts(expr).ok_or(Refused::Predicate)?;
            check_column(&parts, scope)?;
            if literal(low) && literal(high) {
                Ok(())
            } else {
                Err(Refused::Predicate)
            }
        }
        Expr::InList { expr, list, negated } => {
            if *negated {
                return Err(Refused::Predicate);
            }
            let parts = ident_parts(expr).ok_or(Refused::Predicate)?;
            check_column(&parts, scope)?;
            if list.iter().all(literal) {
                Ok(())
            } else {
                Err(Refused::Predicate)
            }
        }
        Expr::Nested(inner) => check_predicate(inner, scope),
        _ => Err(Refused::Predicate),
    }
}

fn aggregate(name: &str) -> bool {
    matches!(name.to_ascii_lowercase().as_str(), "count" | "min" | "max")
}

fn check_projection(item: &SelectItem, scope: &Scope) -> Result<(), Refused> {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return Err(Refused::Projection),
    };
    if let Some(parts) = ident_parts(expr) {
        check_column(&parts, scope)?;
        return Ok(());
    }
    let Expr::Function(function) = expr else {
        return Err(Refused::Projection);
    };
    if function.over.is_some() || function.filter.is_some() || !function.within_group.is_empty() {
        return Err(Refused::Projection);
    }
    let name = function.name.to_string();
    if name.eq_ignore_ascii_case("sum") {
        return Err(Refused::Sum);
    }
    if !aggregate(&name) {
        return Err(Refused::Projection);
    }
    let FunctionArguments::List(list) = &function.args else {
        return Err(Refused::Projection);
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return Err(Refused::Projection);
    }
    let is_count = name.eq_ignore_ascii_case("count");
    if list.args.len() != 1 {
        return Err(Refused::Projection);
    }
    match &list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) if is_count => Ok(()),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
            let parts = ident_parts(inner).ok_or(Refused::Projection)?;
            check_column(&parts, scope)?;
            Ok(())
        }
        _ => Err(Refused::Projection),
    }
}

fn table_name(factor: &TableFactor) -> Result<(String, Option<String>), Refused> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let parts: Vec<String> = name
                .0
                .iter()
                .map(|part| part.as_ident().map(|i| i.value.clone()).unwrap_or_default())
                .collect();
            if parts.len() != 1 {
                return Err(Refused::From);
            }
            Ok((
                parts[0].to_ascii_lowercase(),
                alias.as_ref().map(|a| a.name.value.to_ascii_lowercase()),
            ))
        }
        _ => Err(Refused::From),
    }
}

/// The joined table and its alias. The ON clause is checked separately, after the alias is in scope.
fn join_table(join: &sqlparser::ast::Join) -> Result<(String, Option<String>), Refused> {
    match &join.join_operator {
        JoinOperator::Inner(_) | JoinOperator::Join(_) | JoinOperator::CrossJoin(_) => {}
        _ => return Err(Refused::Join),
    }
    table_name(&join.relation)
}

fn check_join_on(join: &sqlparser::ast::Join, scope: &Scope) -> Result<(), Refused> {
    let constraint = match &join.join_operator {
        JoinOperator::Inner(constraint) | JoinOperator::Join(constraint) => Some(constraint),
        JoinOperator::CrossJoin(_) => None,
        _ => return Err(Refused::Join),
    };
    if let Some(constraint) = constraint {
        match constraint {
            JoinConstraint::On(Expr::BinaryOp {
                left,
                op: sqlparser::ast::BinaryOperator::Eq,
                right,
            }) => {
                let l = ident_parts(left).ok_or(Refused::Join)?;
                let r = ident_parts(right).ok_or(Refused::Join)?;
                check_column(&l, scope)?;
                check_column(&r, scope)?;
            }
            JoinConstraint::Natural => return Err(Refused::Join),
            _ => return Err(Refused::Join),
        }
    }
    Ok(())
}

/// Decides and rewrites, on structure alone: `schema` describes the source tables, so the decision can be cached
/// against the schema version. Storage-class purity is checked separately by [`purity_allows`], live every time.
pub fn plan(sql: &str, schema: &Schema) -> Result<Routed, Refused> {
    let statements = Parser::parse_sql(&DuckDbDialect {}, sql).map_err(|_| Refused::NotOneSelect)?;
    if statements.len() != 1 {
        return Err(Refused::NotOneSelect);
    }
    let Statement::Query(query) = &statements[0] else {
        return Err(Refused::NotOneSelect);
    };
    let mut query: Query = (**query).clone();
    if query.with.is_some() || query.fetch.is_some() || !query.locks.is_empty() {
        return Err(Refused::NotPlainSelect);
    }
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err(Refused::NotPlainSelect);
    };
    if select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || select.exclude.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || select.having.is_some()
        || !select.sort_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || select.qualify.is_some()
        || !select.named_window.is_empty()
        || select.connect_by.is_some()
        || select.value_table_mode.is_some()
    {
        return Err(if select.having.is_some() {
            Refused::Having
        } else {
            Refused::NotPlainSelect
        });
    }

    let (tables, order_columns, decltypes, referenced) = plan_select(select, query.order_by.as_ref(), schema)?;

    // SQLite puts NULLs first on ASC and last on DESC; DuckDB's default does not follow the direction.
    // Stating them explicitly is what makes the two agree (verified by the differential harness).
    if let Some(order_by) = &mut query.order_by {
        match &mut order_by.kind {
            OrderByKind::Expressions(exprs) => {
                for item in exprs {
                    let asc = item.options.asc.unwrap_or(true);
                    item.options.nulls_first = Some(asc);
                }
            }
            OrderByKind::All(_) => return Err(Refused::OrderBy),
        }
    }
    let has_order = order_exprs(&query).is_some_and(|exprs| !exprs.is_empty());
    let total_order = order_columns.iter().any(|(table, index)| {
        schema
            .get(table)
            .and_then(|t| t.columns.get(*index))
            .is_some_and(|c| c.primary_key)
    });
    if query.limit_clause.is_some() && !(has_order && total_order) {
        return Err(Refused::LimitWithoutOrder);
    }

    // Point every table at its mirror view
    if let SetExpr::Select(select) = query.body.as_mut() {
        for from in &mut select.from {
            rename_relation(&mut from.relation);
            for join in &mut from.joins {
                rename_relation(&mut join.relation);
            }
        }
    }
    Ok(Routed {
        sql: query.to_string(),
        tables,
        decltypes,
        referenced,
    })
}

fn order_exprs(query: &Query) -> Option<&Vec<sqlparser::ast::OrderByExpr>> {
    match &query.order_by.as_ref()?.kind {
        OrderByKind::Expressions(exprs) => Some(exprs),
        OrderByKind::All(_) => None,
    }
}

fn rename_relation(factor: &mut TableFactor) {
    if let TableFactor::Table { name, .. } = factor
        && let Some(ObjectNamePart::Identifier(ident)) = name.0.last_mut()
    {
        ident.value = format!("{}_native", ident.value);
        ident.quote_style = None;
    }
}

/// The tables read, the ORDER BY columns resolved, and each output column's declared type
type Planned = (
    Vec<String>,
    Vec<(String, usize)>,
    Vec<Option<String>>,
    Vec<(String, usize)>,
);

fn plan_select(
    select: &Select,
    order_by: Option<&sqlparser::ast::OrderBy>,
    schema: &Schema,
) -> Result<Planned, Refused> {
    if select.from.is_empty() || select.from.len() != 1 {
        return Err(Refused::From);
    }
    let TableWithJoins { relation, joins } = &select.from[0];
    let (first, alias) = table_name(relation)?;
    let mut scope = Scope {
        schema,
        aliases: BTreeMap::new(),
        tables: vec![first.clone()],
        referenced: std::cell::RefCell::new(Vec::new()),
    };
    scope
        .aliases
        .insert(alias.clone().unwrap_or_else(|| first.clone()), first.clone());
    for join in joins {
        let (table, alias) = join_table(join)?;
        scope
            .aliases
            .insert(alias.unwrap_or_else(|| table.clone()), table.clone());
        scope.tables.push(table);
    }
    for join in joins {
        check_join_on(join, &scope)?;
    }
    for table in &scope.tables {
        if !schema.contains_key(table) {
            return Err(Refused::UnknownTable);
        }
    }
    let mut decltypes = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        check_projection(item, &scope)?;
        let expr = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => return Err(Refused::Projection),
        };
        let declared = match ident_parts(expr) {
            Some(parts) => scope.resolve(&parts)?.2.declared,
            None => None,
        };
        decltypes.push(declared);
    }
    if let Some(selection) = &select.selection {
        check_predicate(selection, &scope)?;
    }
    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) => {
            if !modifiers.is_empty() {
                return Err(Refused::GroupBy);
            }
            for expr in exprs {
                let parts = ident_parts(expr).ok_or(Refused::GroupBy)?;
                check_column(&parts, &scope)?;
            }
        }
        _ => return Err(Refused::GroupBy),
    }
    let mut order_columns = Vec::new();
    if let Some(order_by) = order_by {
        if order_by.interpolate.is_some() {
            return Err(Refused::OrderBy);
        }
        let OrderByKind::Expressions(exprs) = &order_by.kind else {
            return Err(Refused::OrderBy);
        };
        for item in exprs {
            if item.with_fill.is_some() {
                return Err(Refused::OrderBy);
            }
            let parts = ident_parts(&item.expr).ok_or(Refused::OrderBy)?;
            order_columns.push(check_column(&parts, &scope)?);
        }
    }
    let referenced = scope.referenced.into_inner();
    Ok((scope.tables, order_columns, decltypes, referenced))
}
/// Describes the source tables the way the gate needs: column order (which the purity string follows), declared
/// type, collation and whether the column is a primary key. Read from SQLite, cached by the caller per schema version.
pub fn describe(conn: &rusqlite::Connection, tables: &[String]) -> anyhow::Result<Schema> {
    let mut schema = Schema::new();
    for table in tables {
        let mut statement = conn.prepare("SELECT name, type, pk FROM pragma_table_xinfo(?1) ORDER BY cid")?;
        let mut rows = statement.query([table])?;
        let mut info = TableInfo::default();
        while let Some(row) = rows.next()? {
            let name: String = row.get(0)?;
            let declared: Option<String> = row.get(1)?;
            let pk: i64 = row.get(2)?;
            // collation is not in pragma_table_xinfo; the split path already reads it from the schema metadata
            let collation = crate::parallel::collation(conn, table, &name)
                .ok_or_else(|| anyhow::anyhow!("no collation for {table}.{name}"))?;
            info.columns.push(ColumnInfo {
                name,
                declared,
                collation,
                primary_key: pk > 0,
            });
        }
        schema.insert(table.to_ascii_lowercase(), info);
    }
    Ok(schema)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, collation: &str, pk: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_owned(),
            declared: Some("INTEGER".to_owned()),
            collation: collation.to_owned(),
            primary_key: pk,
        }
    }

    fn fixture() -> (Schema, BTreeMap<String, Vec<String>>) {
        let mut schema = Schema::new();
        schema.insert(
            "nums".to_owned(),
            TableInfo {
                columns: vec![
                    col("id", "BINARY", true),
                    col("v", "BINARY", false),
                    col("n", "BINARY", false),
                ],
            },
        );
        schema.insert(
            "dim".to_owned(),
            TableInfo {
                columns: vec![col("id", "BINARY", true), col("name", "BINARY", false)],
            },
        );
        schema.insert(
            "txt".to_owned(),
            TableInfo {
                columns: vec![col("id", "BINARY", true), col("nc", "NOCASE", false)],
            },
        );
        let mut purity = BTreeMap::new();
        purity.insert(
            "nums".to_owned(),
            vec!["i".to_owned(), "in".to_owned(), "in".to_owned()],
        );
        purity.insert("dim".to_owned(), vec!["i".to_owned(), "t".to_owned()]);
        purity.insert("txt".to_owned(), vec!["i".to_owned(), "t".to_owned()]);
        (schema, purity)
    }

    /// The two gates as the caller applies them: structure first (cacheable), then live purity
    fn plan_sql(sql: &str) -> Result<Routed, Refused> {
        let (schema, purity) = fixture();
        let routed = plan(sql, &schema)?;
        assert!(purity_allows(&routed, &purity), "fixture columns should be uniform");
        Ok(routed)
    }

    #[test]
    fn the_profitable_shapes_are_admitted_and_rewritten() {
        for sql in [
            "SELECT COUNT(*) AS n FROM nums",
            "SELECT MIN(v) AS lo, MAX(v) AS hi FROM nums",
            "SELECT v, COUNT(*) AS n FROM nums GROUP BY v ORDER BY v",
            "SELECT d.name, COUNT(n.v) AS c FROM nums n JOIN dim d ON d.id=n.id GROUP BY d.name ORDER BY d.name",
            "SELECT id FROM nums WHERE v>10 AND n IS NULL ORDER BY id",
            "SELECT id FROM nums WHERE v IN (1,2,3) ORDER BY id",
            "SELECT id FROM nums ORDER BY id LIMIT 5",
        ] {
            let routed = plan_sql(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
            assert!(routed.sql.contains("_native"), "{sql} -> {}", routed.sql);
        }
    }

    /// The measured divergence: SQLite puts NULLs first on ASC, DuckDB last. Stating them is the fix.
    #[test]
    fn null_order_is_stated_for_both_directions() {
        let asc = plan_sql("SELECT id, n FROM nums ORDER BY n ASC, id").unwrap();
        assert!(asc.sql.contains("NULLS FIRST"), "{}", asc.sql);
        let desc = plan_sql("SELECT id, n FROM nums ORDER BY n DESC, id").unwrap();
        assert!(desc.sql.contains("NULLS LAST"), "{}", desc.sql);
        let bare = plan_sql("SELECT id, n FROM nums ORDER BY n, id").unwrap();
        assert!(bare.sql.contains("NULLS FIRST"), "ASC is the default: {}", bare.sql);
    }

    #[test]
    fn every_measured_divergence_is_refused() {
        let cases: &[(&str, Refused)] = &[
            ("SELECT AVG(v) FROM nums", Refused::Projection),
            ("SELECT CAST(AVG(v) AS BIGINT) FROM nums", Refused::Projection),
            ("SELECT v/2 FROM nums", Refused::Projection),
            ("SELECT typeof(v) FROM nums", Refused::Projection),
            ("SELECT DISTINCT v FROM nums", Refused::NotPlainSelect),
            ("SELECT nc FROM txt", Refused::Collation),
            ("SELECT id FROM txt WHERE nc='a'", Refused::Collation),
            ("SELECT SUM(v) FROM nums", Refused::Sum),
            ("SELECT SUM(name) FROM dim", Refused::Sum),
            ("SELECT v FROM nums LIMIT 3", Refused::LimitWithoutOrder),
            ("SELECT v FROM nums ORDER BY v LIMIT 3", Refused::LimitWithoutOrder),
            ("SELECT n.id FROM nums n LEFT JOIN dim d ON d.id=n.id", Refused::Join),
            (
                "SELECT id FROM nums WHERE v IN (SELECT id FROM dim)",
                Refused::Predicate,
            ),
            ("WITH x AS (SELECT 1) SELECT id FROM nums", Refused::NotPlainSelect),
            ("SELECT id FROM nums UNION SELECT id FROM dim", Refused::NotPlainSelect),
            ("SELECT id FROM nope", Refused::UnknownTable),
            ("SELECT nope FROM nums", Refused::UnknownColumn),
            ("SELECT id FROM nums, dim", Refused::From),
            (
                "SELECT id FROM nums n JOIN dim d ON d.id=n.id",
                Refused::AmbiguousColumn,
            ),
            ("SELECT id FROM nums HAVING COUNT(*)>1", Refused::Having),
            ("SELECT SUM(v) OVER () FROM nums", Refused::Projection),
            ("DELETE FROM nums", Refused::NotOneSelect),
            ("SELECT 1; SELECT 2", Refused::NotOneSelect),
        ];
        for (sql, want) in cases {
            let got = plan_sql(sql).unwrap_err_or_label();
            assert_eq!(got, want.label(), "{sql}");
        }
    }

    /// A column that ever held a real or a blob is not reproducible: float accumulation order and blob ordering
    /// differ, and non-UTF-8 text lands in the mirror as a blob.
    #[test]
    fn a_column_that_was_ever_mixed_is_refused() {
        let (schema, mut purity) = fixture();
        let routed = plan("SELECT v FROM nums", &schema).unwrap();
        purity.insert(
            "nums".to_owned(),
            vec!["i".to_owned(), "inr".to_owned(), "in".to_owned()],
        );
        assert!(!purity_allows(&routed, &purity), "a real made the column mixed");
        purity.insert(
            "nums".to_owned(),
            vec!["i".to_owned(), "it".to_owned(), "in".to_owned()],
        );
        assert!(!purity_allows(&routed, &purity), "text made the column mixed");
        purity.insert(
            "nums".to_owned(),
            vec!["i".to_owned(), "in".to_owned(), "in".to_owned()],
        );
        assert!(purity_allows(&routed, &purity), "integers and NULLs are uniform");
        // a column the mirror has not recorded is refused rather than assumed clean
        purity.insert("nums".to_owned(), vec!["i".to_owned(), String::new(), "in".to_owned()]);
        assert!(!purity_allows(&routed, &purity));
        // SUM is refused on structure alone, whatever the purity says
        assert_eq!(plan("SELECT SUM(v) FROM nums", &schema).unwrap_err(), Refused::Sum);
    }

    trait UnwrapErrOrLabel {
        fn unwrap_err_or_label(self) -> &'static str;
    }
    impl UnwrapErrOrLabel for Result<Routed, Refused> {
        fn unwrap_err_or_label(self) -> &'static str {
            match self {
                Ok(r) => panic!("admitted but should be refused: {}", r.sql),
                Err(e) => e.label(),
            }
        }
    }
}
