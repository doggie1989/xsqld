//! Embeds the git commit as `XSQLD_GIT_COMMIT`: taken from that environment variable when set,
//! else from `git rev-parse --short HEAD`, else `unknown`.

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=XSQLD_GIT_COMMIT");
    let commit = match std::env::var("XSQLD_GIT_COMMIT") {
        Ok(c) if !c.trim().is_empty() => c.trim().to_owned(),
        _ => git_commit().unwrap_or_else(|| "unknown".into()),
    };
    if !commit
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-' || b == b'_')
    {
        panic!("XSQLD_GIT_COMMIT {commit:?}: only letters, digits, '.', '-' and '_'");
    }
    println!("cargo:rustc-env=XSQLD_GIT_COMMIT={commit}");
}

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    let s = String::from_utf8(out.stdout).ok()?;
    (out.status.success() && !s.trim().is_empty()).then(|| s.trim().to_owned())
}

fn git_commit() -> Option<String> {
    let commit = git(&["rev-parse", "--short", "HEAD"])?;
    let mut watch = vec![git(&["rev-parse", "--git-path", "HEAD"])?];
    if let Some(branch) = git(&["symbolic-ref", "-q", "HEAD"]) {
        watch.extend(git(&["rev-parse", "--git-path", &branch]));
    }
    watch.extend(git(&["rev-parse", "--git-path", "packed-refs"]));
    for p in watch.iter().filter(|p| std::path::Path::new(p).exists()) {
        println!("cargo:rerun-if-changed={p}");
    }
    Some(commit)
}
