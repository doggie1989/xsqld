//! Lightweight MySQL / SeekDB SQL lexical scanner: statement classification, multi-statement detection,
//! and Hrana-to-MySQL parameter placeholder rewriting (`?NNN`, `:name`, `$name`, `@name` -> `?`).

use std::collections::{HashMap, HashSet};

use crate::proto;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Only whitespace, comments, and semicolons
    Empty,
    Begin,
    Commit,
    Rollback,
    RollbackTo,
    Savepoint,
    Release,
    Insert,
    Update,
    Delete,
    Select,
    Show,
    Explain,
    Ddl,
    Set,
    Use,
    Other,
}

impl Kind {
    /// DML statements report `affected_row_count` and `rows_written`
    pub fn is_dml(self) -> bool {
        matches!(self, Kind::Insert | Kind::Update | Kind::Delete)
    }

    pub fn is_insert(self) -> bool {
        self == Kind::Insert
    }
}

const BOM: &[u8] = b"\xEF\xBB\xBF";

struct Lexer<'a> {
    s: &'a [u8],
    i: usize,
}

impl<'a> Lexer<'a> {
    fn new(sql: &'a str) -> Self {
        Self {
            s: trimmed(sql).as_bytes(),
            i: 0,
        }
    }

    fn rest(&self) -> &'a [u8] {
        self.s.get(self.i..).unwrap_or_default()
    }

    fn skip_space(&mut self) {
        loop {
            while self.i < self.s.len()
                && (self.s[self.i].is_ascii_whitespace() || self.s[self.i] == 0x0B || self.s[self.i] == b';')
            {
                self.i += 1;
            }
            let rest = self.rest();
            if rest.starts_with(BOM) {
                self.i += BOM.len();
            } else if is_dash_comment(rest) || rest.starts_with(b"#") {
                while self.i < self.s.len() && self.s[self.i] != b'\n' {
                    self.i += 1;
                }
            } else if rest.starts_with(b"/*") {
                match rest[2..].windows(2).position(|w| w == b"*/") {
                    Some(p) => self.i += p + 4,
                    None => self.i = self.s.len(),
                }
            } else {
                return;
            }
        }
    }

    fn next(&mut self) -> Option<Tok> {
        self.skip_space();
        let c = *self.s.get(self.i)?;
        if c.is_ascii_alphabetic() || c == b'_' {
            let start = self.i;
            while self.i < self.s.len() && (self.s[self.i].is_ascii_alphanumeric() || self.s[self.i] == b'_') {
                self.i += 1;
            }
            return Some(Tok::Word(
                String::from_utf8_lossy(&self.s[start..self.i]).to_ascii_uppercase(),
            ));
        }
        match c {
            b'\'' | b'"' => {
                self.i = scan_quoted_string(self.s, self.i);
                Some(Tok::Other)
            }
            b'`' => {
                self.i = scan_backtick(self.s, self.i);
                Some(Tok::Other)
            }
            b'(' => {
                self.i += 1;
                Some(Tok::Open)
            }
            b')' => {
                self.i += 1;
                Some(Tok::Close)
            }
            b',' => {
                self.i += 1;
                Some(Tok::Comma)
            }
            _ => {
                self.i += 1;
                Some(Tok::Other)
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum Tok {
    Word(String),
    Open,
    Close,
    Comma,
    Other,
}

fn is_dash_comment(rest: &[u8]) -> bool {
    rest.starts_with(b"--")
        && (rest.len() == 2 || rest[2].is_ascii_whitespace() || rest[2] == 0x0B)
}

/// Scans `'...'` or `"..."` supporting both `\` escapes and doubled quote escapes `''` / `""`.
/// Returns byte index immediately after the closing quote (or `s.len()` if unterminated).
fn scan_quoted_string(s: &[u8], start: usize) -> usize {
    let quote = s[start];
    let mut i = start + 1;
    while i < s.len() {
        let b = s[i];
        if b == b'\\' {
            i = (i + 2).min(s.len());
            continue;
        }
        if b == quote {
            if s.get(i + 1) == Some(&quote) {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    s.len()
}

/// Scans `` `...` `` supporting doubled backticks ``` `` ```. Backslash is not an escape inside MySQL backticks.
fn scan_backtick(s: &[u8], start: usize) -> usize {
    let mut i = start + 1;
    while i < s.len() {
        if s[i] == b'`' {
            if s.get(i + 1) == Some(&b'`') {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    s.len()
}

pub fn trimmed(sql: &str) -> &str {
    let mut s = sql.trim();
    loop {
        let before = s;
        s = s.trim_start_matches(|c: char| c == ';' || c.is_ascii_whitespace() || c == '\u{feff}');
        if s == before {
            break;
        }
    }
    s
}

pub fn classify(sql: &str) -> Kind {
    let mut lx = Lexer::new(sql);
    let Some(first) = lx.next() else { return Kind::Empty };
    let Tok::Word(w) = first else { return Kind::Other };
    match w.as_str() {
        "BEGIN" => Kind::Begin,
        "START" => match lx.next() {
            Some(Tok::Word(m)) if m == "TRANSACTION" => Kind::Begin,
            _ => Kind::Other,
        },
        "COMMIT" => Kind::Commit,
        "ROLLBACK" => {
            let mut t = lx.next();
            if matches!(&t, Some(Tok::Word(m)) if m == "WORK") {
                t = lx.next();
            }
            if matches!(&t, Some(Tok::Word(m)) if m == "TO") {
                Kind::RollbackTo
            } else {
                Kind::Rollback
            }
        }
        "SAVEPOINT" => Kind::Savepoint,
        "RELEASE" => Kind::Release,
        "INSERT" | "REPLACE" => Kind::Insert,
        "UPDATE" => Kind::Update,
        "DELETE" => Kind::Delete,
        "SELECT" | "VALUES" | "TABLE" => Kind::Select,
        "WITH" => with_body(&mut lx),
        "SHOW" => Kind::Show,
        "EXPLAIN" | "DESC" | "DESCRIBE" => Kind::Explain,
        "CREATE" | "ALTER" | "DROP" | "TRUNCATE" | "RENAME" => Kind::Ddl,
        "SET" => Kind::Set,
        "USE" => Kind::Use,
        _ => Kind::Other,
    }
}

fn with_body(lx: &mut Lexer) -> Kind {
    let mut depth = 0usize;
    let mut name_next = true;
    while let Some(t) = lx.next() {
        match t {
            Tok::Open => depth += 1,
            Tok::Close => depth = depth.saturating_sub(1),
            Tok::Comma if depth == 0 => name_next = true,
            Tok::Word(w) if depth == 0 && name_next => name_next = w == "RECURSIVE",
            Tok::Other if depth == 0 && name_next => name_next = false,
            Tok::Word(w) if depth == 0 => match w.as_str() {
                "SELECT" | "VALUES" | "TABLE" => return Kind::Select,
                "INSERT" | "REPLACE" => return Kind::Insert,
                "UPDATE" => return Kind::Update,
                "DELETE" => return Kind::Delete,
                _ => {}
            },
            _ => {}
        }
    }
    Kind::Other
}

/// Returns true when the SQL contains two or more non-empty semicolon-separated statements.
pub fn many_statements(sql: &str) -> bool {
    let body = trimmed(sql);
    let s = body.as_bytes();
    let mut i = 0usize;
    let mut seen_token = false;
    while i < s.len() {
        let rest = &s[i..];
        if s[i].is_ascii_whitespace() || s[i] == 0x0B {
            i += 1;
        } else if rest.starts_with(BOM) {
            i += BOM.len();
        } else if is_dash_comment(rest) || rest.starts_with(b"#") {
            while i < s.len() && s[i] != b'\n' {
                i += 1;
            }
        } else if rest.starts_with(b"/*") {
            match rest[2..].windows(2).position(|w| w == b"*/") {
                Some(p) => i += p + 4,
                None => return false,
            }
        } else if s[i] == b'\'' || s[i] == b'"' {
            seen_token = true;
            i = scan_quoted_string(s, i);
        } else if s[i] == b'`' {
            seen_token = true;
            i = scan_backtick(s, i);
        } else if s[i] == b';' {
            i += 1;
            if seen_token && classify(&body[i..]) != Kind::Empty {
                return true;
            }
        } else {
            seen_token = true;
            i += 1;
        }
    }
    false
}

/// First ~80 chars of SQL collapsed onto one line for logs (never logs bound values)
pub fn head(sql: &str) -> String {
    let mut out = String::with_capacity(80);
    let mut space = false;
    for c in sql.chars() {
        if c.is_whitespace() {
            space = !out.is_empty();
            continue;
        }
        if space {
            out.push(' ');
            space = false;
        }
        out.push(c);
        if out.len() >= 80 {
            out.push('…');
            break;
        }
    }
    out
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_cont(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn invalid_args(msg: impl Into<String>) -> proto::Error {
    proto::Error::new(
        format!("Arguments do not match SQL parameters: {}", msg.into()),
        "ARGS_INVALID",
    )
}

/// Strips trailing empty `;` (preserving interior SQL verbatim except rewriting Hrana parameter placeholders to `?`)
/// and binds positional (`stmt.args`) or named (`stmt.named_args`) values into MySQL's positional order.
///
/// Rules:
/// - Rejects supplying both `args` and `named_args` (`ARGS_BOTH_POSITIONAL_AND_NAMED`).
/// - Rewrites `?` and `?NNN` (1-based) using `stmt.args`. Rejects missing values or extra unused `stmt.args`.
/// - Rewrites `:name`, `$name`, and (only when `stmt.named_args` matches) `@name` to `?`.
/// - Preserves MySQL `:=`, `@@sysvar`, unmatched `@user_var`, strings, backticks, and comments untouched.
/// - Duplicate names in `named_args` take the last value; unused unique names in `named_args` fail with `ARGS_INVALID`.
pub fn rewrite_params(sql: &str, stmt: &proto::Stmt) -> Result<(String, Vec<proto::Value>), proto::Error> {
    if !stmt.args.is_empty() && !stmt.named_args.is_empty() {
        return Err(proto::Error::new(
            "Specifying both positional and named arguments is not supported",
            "ARGS_BOTH_POSITIONAL_AND_NAMED",
        ));
    }

    // Also strip trailing semicolons cleanly outside strings/comments by using the trimmed input
    let body = trimmed(sql);
    let s = body.as_bytes();
    let use_named = !stmt.named_args.is_empty();

    let mut named_map: HashMap<&str, &proto::Value> = HashMap::new();
    let mut provided_unique: Vec<&str> = Vec::new();
    if use_named {
        for na in &stmt.named_args {
            let clean = na.name.strip_prefix([':', '$', '@']).unwrap_or(&na.name);
            if !named_map.contains_key(clean) {
                provided_unique.push(clean);
            }
            named_map.insert(clean, &na.value);
        }
    }

    let mut out = String::with_capacity(body.len());
    let mut bound: Vec<proto::Value> = Vec::new();
    let mut used_named: HashSet<&str> = HashSet::new();
    let mut next_pos: usize = 1;
    let mut max_pos_used: usize = 0;

    let mut i = 0usize;
    let mut copy_from = 0usize;
    while i < s.len() {
        let rest = &s[i..];
        if is_dash_comment(rest) || rest.starts_with(b"#") {
            while i < s.len() && s[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if rest.starts_with(b"/*") {
            match rest[2..].windows(2).position(|w| w == b"*/") {
                Some(p) => i += p + 4,
                None => i = s.len(),
            }
            continue;
        }
        if s[i] == b'\'' || s[i] == b'"' {
            i = scan_quoted_string(s, i);
            continue;
        }
        if s[i] == b'`' {
            i = scan_backtick(s, i);
            continue;
        }

        // Positional placeholder `?` or `?NNN`
        if s[i] == b'?' {
            out.push_str(&body[copy_from..i]);
            i += 1;
            let num_start = i;
            while i < s.len() && s[i].is_ascii_digit() {
                i += 1;
            }
            let pos = if i > num_start {
                let digits = &body[num_start..i];
                let p: usize = digits.parse().map_err(|_| {
                    invalid_args(format!(
                        "invalid parameter ?{digits}: expected a numerical position after `?`"
                    ))
                })?;
                if p == 0 {
                    return Err(invalid_args("invalid parameter ?0: positions are 1-based"));
                }
                p
            } else {
                let p = next_pos;
                next_pos += 1;
                p
            };
            if use_named {
                return Err(invalid_args(format!("value for parameter {pos} not found")));
            }
            let val = stmt
                .args
                .get(pos - 1)
                .ok_or_else(|| invalid_args(format!("value for parameter {pos} not found")))?;
            max_pos_used = max_pos_used.max(pos);
            bound.push(val.clone());
            out.push('?');
            copy_from = i;
            continue;
        }

        // Named placeholder `:name` (avoid `:=` and `::`)
        if s[i] == b':' {
            let prev_is_colon = i > 0 && s[i - 1] == b':';
            if !prev_is_colon && s.get(i + 1).is_some_and(|&b| is_ident_start(b)) {
                let name_start = i + 1;
                let mut name_end = name_start + 1;
                while name_end < s.len() && is_ident_cont(s[name_end]) {
                    name_end += 1;
                }
                let raw_tok = &body[i..name_end];
                let clean = &body[name_start..name_end];
                let val = named_map
                    .get(clean)
                    .copied()
                    .ok_or_else(|| invalid_args(format!("value for parameter {raw_tok} not found")))?;
                out.push_str(&body[copy_from..i]);
                out.push('?');
                bound.push(val.clone());
                used_named.insert(clean);
                i = name_end;
                copy_from = i;
                continue;
            }
            i += 1;
            continue;
        }

        // Named placeholder `$name` (avoid mid-identifier `$` such as column `a$b`)
        if s[i] == b'$' {
            let prev_is_ident = i > 0 && (is_ident_cont(s[i - 1]) || s[i - 1] == b'$');
            if !prev_is_ident && s.get(i + 1).is_some_and(|&b| is_ident_start(b)) {
                let name_start = i + 1;
                let mut name_end = name_start + 1;
                while name_end < s.len() && is_ident_cont(s[name_end]) {
                    name_end += 1;
                }
                let raw_tok = &body[i..name_end];
                let clean = &body[name_start..name_end];
                let val = named_map
                    .get(clean)
                    .copied()
                    .ok_or_else(|| invalid_args(format!("value for parameter {raw_tok} not found")))?;
                out.push_str(&body[copy_from..i]);
                out.push('?');
                bound.push(val.clone());
                used_named.insert(clean);
                i = name_end;
                copy_from = i;
                continue;
            }
            i += 1;
            continue;
        }

        // Named placeholder `@name` — only rewrite when `named_args` matches it; never touch `@@sysvar` or `@user_var`
        if s[i] == b'@' && use_named {
            let prev_at = i > 0 && s[i - 1] == b'@';
            let prev_ident = i > 0 && is_ident_cont(s[i - 1]);
            let next_at = s.get(i + 1) == Some(&b'@');
            if !prev_at && !prev_ident && !next_at && s.get(i + 1).is_some_and(|&b| is_ident_start(b)) {
                let name_start = i + 1;
                let mut name_end = name_start + 1;
                while name_end < s.len() && is_ident_cont(s[name_end]) {
                    name_end += 1;
                }
                let clean = &body[name_start..name_end];
                if let Some(&val) = named_map.get(clean) {
                    out.push_str(&body[copy_from..i]);
                    out.push('?');
                    bound.push(val.clone());
                    used_named.insert(clean);
                    i = name_end;
                    copy_from = i;
                    continue;
                }
            }
            i += 1;
            continue;
        }

        i += 1;
    }

    if copy_from < s.len() {
        out.push_str(&body[copy_from..]);
    }

    // Strip a trailing single `;` safely: find trailing semicolon outside strings/comments
    trim_trailing_semicolons(&mut out);

    if use_named {
        if let Some(unused) = provided_unique.iter().find(|n| !used_named.contains(*n)) {
            return Err(invalid_args(format!(
                "too many parameters: unused named argument `{unused}`"
            )));
        }
    } else if stmt.args.len() > max_pos_used {
        return Err(invalid_args(format!(
            "too many parameters, expected {max_pos_used} found {}",
            stmt.args.len()
        )));
    }

    Ok((out, bound))
}

fn trim_trailing_semicolons(sql: &mut String) {
    loop {
        let trimmed_ws = sql.trim_end();
        if let Some(without_semi) = trimmed_ws.strip_suffix(';') {
            let new_len = without_semi.trim_end().len();
            sql.truncate(new_len);
        } else {
            let new_len = trimmed_ws.len();
            sql.truncate(new_len);
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{NamedArg, Stmt, Value};

    #[test]
    fn classifies_mysql_statements() {
        assert_eq!(classify("   \n; -- hi\n # hash\n /* block */ ;  "), Kind::Empty);
        assert_eq!(classify("BEGIN"), Kind::Begin);
        assert_eq!(classify("START TRANSACTION"), Kind::Begin);
        assert_eq!(classify("COMMIT"), Kind::Commit);
        assert_eq!(classify("ROLLBACK"), Kind::Rollback);
        assert_eq!(classify("ROLLBACK WORK"), Kind::Rollback);
        assert_eq!(classify("ROLLBACK TO sp1"), Kind::RollbackTo);
        assert_eq!(classify("ROLLBACK WORK TO SAVEPOINT sp1"), Kind::RollbackTo);
        assert_eq!(classify("SAVEPOINT s1"), Kind::Savepoint);
        assert_eq!(classify("RELEASE SAVEPOINT s1"), Kind::Release);
        assert_eq!(classify("INSERT INTO t VALUES (1)"), Kind::Insert);
        assert_eq!(classify("REPLACE INTO t VALUES (1)"), Kind::Insert);
        assert_eq!(classify("UPDATE t SET a = 1"), Kind::Update);
        assert_eq!(classify("DELETE FROM t"), Kind::Delete);
        assert_eq!(classify("SELECT 1"), Kind::Select);
        assert_eq!(classify("TABLE t"), Kind::Select);
        assert_eq!(classify("WITH c AS (SELECT 1) SELECT * FROM c"), Kind::Select);
        assert_eq!(classify("WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c"), Kind::Insert);
        assert_eq!(classify("SHOW TABLES"), Kind::Show);
        assert_eq!(classify("EXPLAIN SELECT 1"), Kind::Explain);
        assert_eq!(classify("DESC t"), Kind::Explain);
        assert_eq!(classify("CREATE TABLE t(a INT)"), Kind::Ddl);
        assert_eq!(classify("ALTER TABLE t ADD COLUMN b INT"), Kind::Ddl);
        assert_eq!(classify("DROP DATABASE foo"), Kind::Ddl);
        assert_eq!(classify("SET @a := 1"), Kind::Set);
        assert_eq!(classify("USE db1"), Kind::Use);
    }

    #[test]
    fn detects_multiple_statements_with_mysql_lexing() {
        assert!(!many_statements("SELECT 1;"));
        assert!(!many_statements("SELECT 'a;b'"));
        assert!(!many_statements("SELECT 'it\\'s;ok'"));
        assert!(!many_statements("SELECT `semi;colon` FROM t; # comment; trailing"));
        assert!(!many_statements("SELECT 1 -- ; SELECT 2\n"));
        assert!(many_statements("SELECT 1; SELECT 2"));
        assert!(many_statements("SET @a = 1; SELECT @a"));
    }

    #[test]
    fn rewrites_positional_and_named_params_and_preserves_mysql_vars() {
        // Positional & numbered placeholders
        let stmt = Stmt {
            sql: Some("SELECT ?2, ?1, '?', /* :ignore */ ?3;".into()),
            sql_id: None,
            args: vec![Value::Integer(10), Value::Text("hi".into()), Value::Null],
            named_args: vec![],
            want_rows: None,
        };
        let (q, vals) = rewrite_params(stmt.sql.as_deref().unwrap(), &stmt).unwrap();
        assert_eq!(q, "SELECT ?, ?, '?', /* :ignore */ ?");
        assert_eq!(vals, vec![Value::Text("hi".into()), Value::Integer(10), Value::Null]);

        // Named placeholders with prefix variants and duplicate override
        let stmt = Stmt {
            sql: Some("SELECT :id, $tag, @user_v, :id WHERE 1=1".into()),
            sql_id: None,
            args: vec![],
            named_args: vec![
                NamedArg {
                    name: "id".into(),
                    value: Value::Integer(1),
                },
                NamedArg {
                    name: ":id".into(),
                    value: Value::Integer(2),
                },
                NamedArg {
                    name: "$tag".into(),
                    value: Value::Text("ok".into()),
                },
                NamedArg {
                    name: "@user_v".into(),
                    value: Value::Float(3.5),
                },
            ],
            want_rows: None,
        };
        let (q, vals) = rewrite_params(stmt.sql.as_deref().unwrap(), &stmt).unwrap();
        assert_eq!(q, "SELECT ?, ?, ?, ? WHERE 1=1");
        assert_eq!(
            vals,
            vec![
                Value::Integer(2),
                Value::Text("ok".into()),
                Value::Float(3.5),
                Value::Integer(2),
            ]
        );

        // MySQL @user_var, @@sysvar, and := assignment remain intact
        let stmt = Stmt {
            sql: Some("SET @x := 42 + @@max_connections".into()),
            sql_id: None,
            args: vec![],
            named_args: vec![],
            want_rows: None,
        };
        let (q, vals) = rewrite_params(stmt.sql.as_deref().unwrap(), &stmt).unwrap();
        assert_eq!(q, "SET @x := 42 + @@max_connections");
        assert!(vals.is_empty());

        // Mid-identifier $ stays literal (e.g. col$1)
        let stmt = Stmt {
            sql: Some("SELECT v$session FROM dual".into()),
            sql_id: None,
            args: vec![],
            named_args: vec![],
            want_rows: None,
        };
        let (q, vals) = rewrite_params(stmt.sql.as_deref().unwrap(), &stmt).unwrap();
        assert_eq!(q, "SELECT v$session FROM dual");
        assert!(vals.is_empty());
    }

    #[test]
    fn rejects_invalid_parameter_combinations() {
        let missing = Stmt {
            sql: Some("SELECT :nope".into()),
            sql_id: None,
            args: vec![],
            named_args: vec![],
            want_rows: None,
        };
        assert_eq!(
            rewrite_params("SELECT :nope", &missing).unwrap_err().code,
            "ARGS_INVALID"
        );

        let extra_pos = Stmt {
            sql: Some("SELECT 1".into()),
            sql_id: None,
            args: vec![Value::Integer(1)],
            named_args: vec![],
            want_rows: None,
        };
        assert_eq!(
            rewrite_params("SELECT 1", &extra_pos).unwrap_err().code,
            "ARGS_INVALID"
        );

        let extra_named = Stmt {
            sql: Some("SELECT :a".into()),
            sql_id: None,
            args: vec![],
            named_args: vec![
                NamedArg {
                    name: "a".into(),
                    value: Value::Integer(1),
                },
                NamedArg {
                    name: "b".into(),
                    value: Value::Integer(2),
                },
            ],
            want_rows: None,
        };
        assert_eq!(
            rewrite_params("SELECT :a", &extra_named).unwrap_err().code,
            "ARGS_INVALID"
        );

        let both = Stmt {
            sql: Some("SELECT ?".into()),
            sql_id: None,
            args: vec![Value::Integer(1)],
            named_args: vec![NamedArg {
                name: "a".into(),
                value: Value::Integer(1),
            }],
            want_rows: None,
        };
        assert_eq!(
            rewrite_params("SELECT ?", &both).unwrap_err().code,
            "ARGS_BOTH_POSITIONAL_AND_NAMED"
        );
    }
}
