//! Pure unit tests for `Bucket::from_url` — no network, no I/O.

use overture_stac::s3::Bucket;

fn err_msg<T>(r: Result<T, anyhow::Error>) -> String {
    match r {
        Ok(_) => panic!("expected error, got Ok"),
        Err(e) => format!("{e:#}"),
    }
}

#[test]
fn s3_bucket_root_ok() {
    let b = Bucket::from_url("s3://overturemaps-us-west-2").expect("parse s3 URI");
    assert_eq!(b.name, "overturemaps-us-west-2");
}

#[test]
fn s3_bucket_root_trailing_slash_ok() {
    let b = Bucket::from_url("s3://overturemaps-us-west-2/").expect("parse s3 URI with slash");
    assert_eq!(b.name, "overturemaps-us-west-2");
}

#[test]
fn s3_with_path_rejected() {
    let msg = err_msg(Bucket::from_url("s3://overturemaps-us-west-2/release/"));
    assert!(
        msg.contains("URI must point at bucket root"),
        "unexpected error: {msg}"
    );
}

#[test]
fn invalid_uri_rejected() {
    let msg = err_msg(Bucket::from_url("not-a-uri"));
    assert!(msg.contains("parsing URI"), "unexpected error: {msg}");
}

#[test]
fn unsupported_scheme_rejected() {
    // `ftp://` isn't in object_store's registry — expect a construction error.
    let msg = err_msg(Bucket::from_url("ftp://example.com"));
    assert!(
        msg.contains("initialising object store"),
        "unexpected error: {msg}"
    );
}
