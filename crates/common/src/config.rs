//! Upstream source configuration.
//!
//! ```toml
//! [sources.provider]
//! url = "https://provider.example:443"
//! headers = { x-token = "${PROVIDER_TOKEN}" }
//!
//! [sources.own]
//! url = "https://fullnode.example.com:443"
//! reconnect_before = "235m"
//! ```
//!
//! Header values may reference environment variables as `${NAME}`; a missing
//! variable is an error so a misconfigured secret fails at startup.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourcesFile {
    pub sources: BTreeMap<String, RawSource>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSource {
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Open a replacement stream this long after the current one started and
    /// switch over once it delivers (make-before-break).
    pub reconnect_before: Option<String>,
}

/// A source with environment references resolved.
#[derive(Clone, Debug)]
pub struct Source {
    pub name: String,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub reconnect_before: Option<Duration>,
}

impl SourcesFile {
    pub fn parse(text: &str) -> Result<Self> {
        let file: SourcesFile = toml::from_str(text).context("parse sources config")?;
        for name in file.sources.keys() {
            validate_token(name).with_context(|| format!("source name {name:?}"))?;
        }
        Ok(file)
    }

    /// Resolve the named source, expanding `${ENV}` references with `env`.
    pub fn resolve(&self, name: &str, env: impl Fn(&str) -> Option<String>) -> Result<Source> {
        let raw = self
            .sources
            .get(name)
            .ok_or_else(|| anyhow!("unknown source {name:?}"))?;
        let headers = raw
            .headers
            .iter()
            .map(|(k, v)| {
                let value = expand_env(v, &env).with_context(|| format!("header {k}"))?;
                Ok((k.to_ascii_lowercase(), value))
            })
            .collect::<Result<_>>()?;
        let reconnect_before = raw
            .reconnect_before
            .as_deref()
            .map(parse_duration)
            .transpose()
            .context("reconnect_before")?;
        Ok(Source {
            name: name.to_owned(),
            url: raw.url.clone(),
            headers,
            reconnect_before,
        })
    }
}

/// Subject tokens must not contain NATS wildcards or separators.
pub fn validate_token(token: &str) -> Result<()> {
    if token.is_empty()
        || !token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        bail!("must be non-empty and contain only [A-Za-z0-9_-]");
    }
    Ok(())
}

/// Expand `${NAME}` references. `$` not followed by `{` is literal.
pub fn expand_env(value: &str, env: impl Fn(&str) -> Option<String>) -> Result<String> {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .ok_or_else(|| anyhow!("unterminated ${{ in {value:?}"))?;
        let name = &after[..end];
        if name.is_empty() {
            bail!("empty variable name in {value:?}");
        }
        let resolved =
            env(name).ok_or_else(|| anyhow!("environment variable {name} is not set"))?;
        out.push_str(&resolved);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Parse `"25s"`, `"500ms"` or `"2m"`.
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    let split = s
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| anyhow!("missing unit in {s:?}"))?;
    let (num, unit) = s.split_at(split);
    let n: u64 = num.parse().with_context(|| format!("number in {s:?}"))?;
    Ok(match unit {
        "ms" => Duration::from_millis(n),
        "s" => Duration::from_secs(n),
        "m" => Duration::from_secs(n * 60),
        _ => bail!("unknown unit {unit:?} in {s:?}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(name: &str) -> Option<String> {
        match name {
            "TOKEN" => Some("secret".into()),
            "EMPTY" => Some(String::new()),
            _ => None,
        }
    }

    #[test]
    fn expands_references() {
        assert_eq!(expand_env("${TOKEN}", env).unwrap(), "secret");
        assert_eq!(expand_env("a-${TOKEN}-b", env).unwrap(), "a-secret-b");
        assert_eq!(expand_env("${TOKEN}${TOKEN}", env).unwrap(), "secretsecret");
        assert_eq!(expand_env("x${EMPTY}y", env).unwrap(), "xy");
        assert_eq!(expand_env("$5 and $TOKEN", env).unwrap(), "$5 and $TOKEN");
        assert_eq!(expand_env("plain", env).unwrap(), "plain");
    }

    #[test]
    fn rejects_bad_references() {
        assert!(expand_env("${MISSING}", env).is_err());
        assert!(expand_env("${TOKEN", env).is_err());
        assert!(expand_env("${}", env).is_err());
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("25s").unwrap(), Duration::from_secs(25));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert!(parse_duration("25").is_err());
        assert!(parse_duration("s").is_err());
        assert!(parse_duration("5h").is_err());
    }

    #[test]
    fn resolves_sources() {
        let file = SourcesFile::parse(
            r#"
            [sources.provider]
            url = "https://qn.example:443"
            headers = { X-Token = "${TOKEN}" }

            [sources.own]
            url = "https://own.example:443"
            reconnect_before = "235m"
            "#,
        )
        .unwrap();

        let qn = file.resolve("provider", env).unwrap();
        assert_eq!(
            qn.headers.get("x-token").map(String::as_str),
            Some("secret")
        );
        assert_eq!(qn.reconnect_before, None);

        let own = file.resolve("own", env).unwrap();
        assert!(own.headers.is_empty());
        assert_eq!(own.reconnect_before, Some(Duration::from_secs(235 * 60)));

        assert!(file.resolve("nope", env).is_err());
        assert!(file.resolve("provider", |_| None).is_err());
    }

    #[test]
    fn rejects_bad_source_names_and_fields() {
        assert!(SourcesFile::parse("[sources.\"a.b\"]\nurl = \"x\"").is_err());
        assert!(SourcesFile::parse("[sources.a]\nurl = \"x\"\nbogus = 1").is_err());
    }

    #[test]
    fn example_config_parses() {
        let file = SourcesFile::parse(include_str!("../../../sources.example.toml")).unwrap();
        for name in ["provider", "own", "mysten"] {
            file.resolve(name, |_| Some("x".into())).unwrap();
        }
    }
}
