//! HTTP fetches against a published STAC catalog.

use std::collections::HashMap;
use std::sync::LazyLock;

use regex::Regex;
use serde::Deserialize;

use crate::{Error, Result, ResultExt};

const USER_AGENT: &str = concat!("overture-stac/", env!("CARGO_PKG_VERSION"));

/// Overture release calendar. Shipped releases carry a `v<semver>` `schemaVersion`, upcoming ones are null.
pub const RELEASE_CALENDAR_URL: &str = "https://docs.overturemaps.org/release-calendar.json";

fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .build()
        .context("building HTTP client")
}

async fn fetch_json(url: &str) -> Result<serde_json::Value> {
    http_client()?
        .get(url)
        .send()
        .await
        .with_context(|| format!("fetching {url}"))?
        .error_for_status()
        .with_context(|| format!("HTTP error from {url}"))?
        .json()
        .await
        .with_context(|| format!("parsing JSON from {url}"))
}

/// Read `schema:version` from `<root_href>/<release_version>/catalog.json`.
/// Returns `None` when the field is absent or explicitly null.
pub async fn fetch_schema_version(
    root_href: &str,
    release_version: &str,
) -> Result<Option<String>> {
    let url = format!(
        "{}/{}/catalog.json",
        root_href.trim_end_matches('/'),
        release_version
    );
    let body = fetch_json(&url).await?;
    Ok(body
        .get("schema:version")
        .and_then(|v| v.as_str())
        .map(String::from))
}

/// Look up `release_id` in the release calendar; returns its `schemaVersion` without the `v`, or `None` if absent/unshipped.
pub async fn resolve_schema_version_from_calendar(release_id: &str) -> Result<Option<String>> {
    static CALENDAR_CACHE: LazyLock<tokio::sync::OnceCell<Option<ReleaseCalendar>>> =
        LazyLock::new(tokio::sync::OnceCell::new);

    let cached = CALENDAR_CACHE
        .get_or_init(|| async {
            match fetch_json(RELEASE_CALENDAR_URL)
                .await
                .and_then(parse_release_calendar)
            {
                Ok(calendar) => Some(calendar),
                Err(err) => {
                    tracing::warn!(
                        ?err,
                        "failed to load release calendar; schema:version stays null for the rest of this run"
                    );
                    None
                }
            }
        })
        .await;

    let Some(calendar) = cached else {
        return Ok(None);
    };

    match calendar.get(release_id) {
        None => {
            tracing::warn!(
                release_id,
                "release not in release calendar; schema:version stays null"
            );
            Ok(None)
        }
        Some(None) => {
            tracing::warn!(
                release_id,
                "release calendar has no schemaVersion yet; schema:version stays null"
            );
            Ok(None)
        }
        Some(Some(v)) => Ok(Some(v.clone())),
    }
}

/// `dataVersion` -> `schemaVersion` without the `v` prefix (`None` when null).
type ReleaseCalendar = HashMap<String, Option<String>>;

// Mirrors https://docs.overturemaps.org/release-calendar.schema.json.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CalendarDoc {
    #[serde(rename = "$schema")]
    _schema: Option<String>,
    releases: Vec<CalendarEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CalendarEntry {
    #[serde(rename = "date")]
    _date: String,
    data_version: String,
    // `Option<Option<_>>` so a missing key (invalid) differs from an explicit null (unshipped).
    #[serde(default, deserialize_with = "present")]
    schema_version: Option<Option<String>>,
    #[serde(rename = "majorChangeMonth")]
    _major_change_month: Option<bool>,
}

fn present<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<Option<String>>, D::Error> {
    Option::<String>::deserialize(d).map(Some)
}

static DATA_VERSION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\d{4}-\d{2}-\d{2}(-(alpha|beta))?\.\d+$").expect("static regex")
});
static SCHEMA_VERSION_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^v\d+\.\d+\.\d+(-(alpha|beta))?$").expect("static regex"));

fn parse_release_calendar(body: serde_json::Value) -> Result<ReleaseCalendar> {
    let bad = |msg: String| Error::MalformedReleaseCalendar(msg);
    let doc: CalendarDoc = serde_json::from_value(body).map_err(|e| bad(e.to_string()))?;

    let mut out = ReleaseCalendar::with_capacity(doc.releases.len());
    for entry in doc.releases {
        let data_version = entry.data_version;
        if !DATA_VERSION_RE.is_match(&data_version) {
            return Err(bad(format!("invalid dataVersion {data_version:?}")));
        }
        let schema_version = match entry.schema_version {
            None => return Err(bad(format!("{data_version}: missing schemaVersion"))),
            Some(None) => None,
            Some(Some(v)) if SCHEMA_VERSION_RE.is_match(&v) => Some(v[1..].to_string()),
            Some(Some(v)) => {
                return Err(bad(format!("{data_version}: invalid schemaVersion {v:?}")))
            }
        };
        if out.contains_key(&data_version) {
            return Err(bad(format!("duplicate dataVersion {data_version}")));
        }
        out.insert(data_version, schema_version);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const LIVE_CALENDAR: &str = include_str!("../../tests/fixtures/release-calendar.json");
    const CALENDAR_SCHEMA: &str = include_str!("../../tests/fixtures/release-calendar.schema.json");

    fn entry(data_version: &str, schema_version: serde_json::Value) -> serde_json::Value {
        json!({"date": "2026-01-01", "dataVersion": data_version, "schemaVersion": schema_version})
    }

    fn parse_err(body: serde_json::Value) -> String {
        match parse_release_calendar(body) {
            Ok(_) => panic!("expected a parse error"),
            Err(err) => err.to_string(),
        }
    }

    #[test]
    fn parses_calendar_entries() {
        let body = json!({
            "$schema": "https://docs.overturemaps.org/release-calendar.schema.json",
            "releases": [
                entry("2024-04-16-beta.0", json!("v0.10.0-beta")),
                entry("2026-08-19.0", json!("v1.18.0")),
                {"date": "2026-10-21", "dataVersion": "2026-10-21.0", "schemaVersion": null, "majorChangeMonth": false},
            ]
        });
        let cal = parse_release_calendar(body).unwrap();
        assert_eq!(cal.len(), 3);
        assert_eq!(cal["2026-08-19.0"].as_deref(), Some("1.18.0"));
        assert_eq!(cal["2024-04-16-beta.0"].as_deref(), Some("0.10.0-beta"));
        assert_eq!(cal["2026-10-21.0"], None);
        assert!(!cal.contains_key("2026-12-16.0"));
    }

    #[test]
    fn rejects_missing_releases_key() {
        assert!(parse_err(json!({})).contains("releases"));
    }

    #[test]
    fn rejects_bad_schema_version() {
        let err = parse_err(json!({"releases": [entry("2026-08-19.0", json!("1.18.0"))]}));
        assert!(err.contains("invalid schemaVersion"), "{err}");
    }

    #[test]
    fn rejects_bad_data_version() {
        let err = parse_err(json!({"releases": [entry("2026-08-19", json!("v1.18.0"))]}));
        assert!(err.contains("invalid dataVersion"), "{err}");
    }

    #[test]
    fn rejects_missing_schema_version() {
        let err =
            parse_err(json!({"releases": [{"date": "2026-08-19", "dataVersion": "2026-08-19.0"}]}));
        assert!(err.contains("missing schemaVersion"), "{err}");
    }

    #[test]
    fn rejects_missing_required_field() {
        let err = parse_err(
            json!({"releases": [{"dataVersion": "2026-08-19.0", "schemaVersion": null}]}),
        );
        assert!(err.contains("date"), "{err}");
    }

    #[test]
    fn rejects_unknown_field() {
        let mut e = entry("2026-08-19.0", json!("v1.18.0"));
        e["extra"] = json!(1);
        assert!(parse_err(json!({"releases": [e]})).contains("extra"));
        assert!(parse_err(json!({"releases": [], "extra": 1})).contains("extra"));
    }

    #[test]
    fn rejects_duplicate_data_version() {
        let err = parse_err(json!({"releases": [
            entry("2026-08-19.0", json!("v1.18.0")),
            entry("2026-08-19.0", json!("v1.17.0")),
        ]}));
        assert!(err.contains("duplicate dataVersion"), "{err}");
    }

    #[test]
    fn live_calendar_fixture_matches_schema_and_parses() {
        let doc: serde_json::Value = serde_json::from_str(LIVE_CALENDAR).unwrap();
        let schema: serde_json::Value = serde_json::from_str(CALENDAR_SCHEMA).unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        let errors: Vec<String> = validator.iter_errors(&doc).map(|e| e.to_string()).collect();
        assert!(errors.is_empty(), "fixture violates schema: {errors:?}");

        let cal = parse_release_calendar(doc).unwrap();
        assert_eq!(cal["2026-09-23.0"].as_deref(), Some("2.0.0"));
        assert_eq!(cal["2026-09-23.1"].as_deref(), Some("2.0.0"));
    }
}
