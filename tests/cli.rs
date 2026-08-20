//! CLI smoke tests via `assert_cmd`. Verify argparse, dispatch, and validation error
//! messages — no network, no I/O.

use assert_cmd::Command;
use predicates::prelude::*;

fn cli() -> Command {
    Command::cargo_bin("overture-stac").expect("binary built")
}

#[test]
fn help_lists_build_subcommand() {
    cli()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("build"));
}

#[test]
fn no_args_shows_help_and_exits_nonzero() {
    cli().assert().failure();
}

#[test]
fn build_help_lists_all_flags() {
    let assert = cli().args(["build", "--help"]).assert().success();
    let stdout = String::from_utf8(assert.get_output().stdout.clone()).unwrap();
    for flag in [
        "--output",
        "--data-uri",
        "--extras-uri",
        "--debug",
        "--concurrency",
        "--release-version",
        "--schema-version",
        "--root-href",
    ] {
        assert!(stdout.contains(flag), "help missing flag {flag}");
    }
}

#[test]
fn release_without_schema_is_rejected() {
    cli()
        .args(["build", "--release-version", "2026-07-22.0"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--schema-version is required"));
}

#[test]
fn malformed_release_is_rejected() {
    cli()
        .args([
            "build",
            "--release-version",
            "not-a-release",
            "--schema-version",
            "1.18.0",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "--release-version must be in format",
        ));
}

#[test]
fn malformed_schema_is_rejected() {
    cli()
        .args([
            "build",
            "--release-version",
            "2026-07-22.0",
            "--schema-version",
            "not.a.version.string",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "--schema-version must be in format",
        ));
}
