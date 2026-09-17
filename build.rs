//! Capture git commit + branch at build time so the CLI can stamp the STAC catalogs
//! it writes with `vcs:*` provenance fields (see the VCS STAC extension).

use std::process::Command;

fn main() {
    let commit = git_output(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let branch =
        git_output(&["rev-parse", "--abbrev-ref", "HEAD"]).unwrap_or_else(|| "unknown".into());

    println!("cargo:rustc-env=GIT_COMMIT={commit}");
    println!("cargo:rustc-env=GIT_BRANCH={branch}");

    // Rebuild when git state changes so the env vars stay fresh.
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/heads");
}

fn git_output(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?;
    Some(s.trim().to_string())
}
