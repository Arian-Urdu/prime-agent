//! Prime CLI directory contexts: the Prime team a session directory selects
//! through `PRIME_CONTEXT` or the nearest `.prime/context.json` (written by
//! `prime switch <team> --local` and `prime config use <context> --local`),
//! resolved the way the prime CLI and its SDKs resolve it.

use std::path::Path;

use serde_json::Value;

use super::prime_inference::{normalize_base_url, string_field, DEFAULT_PRIME_API_BASE_URL};

/// The team a directory context selects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimeDirectoryTeam {
    /// `None` is the personal account.
    pub team_id: Option<String>,
    /// The team's display name, when the selection carries one.
    pub name: Option<String>,
    /// Where the selection came from: the pin file, or `PRIME_CONTEXT`.
    pub source: String,
}

/// A JSON object file, or the error naming the file.
fn read_object(path: &Path) -> Result<Value, String> {
    let content =
        std::fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?;
    match serde_json::from_str(&content) {
        Ok(object @ Value::Object(_)) => Ok(object),
        Ok(
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_),
        ) => Err(format!("{}: expected a JSON object", path.display())),
        Err(error) => Err(format!("{}: {error}", path.display())),
    }
}

/// The team `PRIME_CONTEXT` (`prime_context`) or the nearest
/// `.prime/context.json` at or above `cwd` selects, or `None` when neither
/// applies. As in the prime CLI, the walk stops at `home` (whose `.prime` is
/// the global config) and skips symlinks and files another user owns, a pin's
/// `team_id` (null: the personal account) wins over its `context`, and a
/// saved context resolves from `home/.prime/environments`. A saved context
/// that targets a non-production API selects nothing: Prime Inference is
/// production-only.
///
/// # Errors
///
/// A malformed pin, or a pin or `PRIME_CONTEXT` naming a missing, unreadable
/// or malformed saved context.
pub(crate) fn resolve_directory_team(
    cwd: &Path,
    home: &Path,
    prime_context: Option<&str>,
) -> Result<Option<PrimeDirectoryTeam>, String> {
    let explicit = prime_context
        .map(str::trim)
        .filter(|context| !context.is_empty());
    let (context, source) = if let Some(context) = explicit {
        (context.to_string(), "PRIME_CONTEXT".to_string())
    } else {
        let stop = std::fs::canonicalize(home).unwrap_or_else(|_| home.to_path_buf());
        let start = std::fs::canonicalize(cwd).unwrap_or_else(|_| cwd.to_path_buf());
        let found = start
            .ancestors()
            .take_while(|directory| *directory != stop)
            .find_map(|directory| {
                let prime = directory.join(".prime");
                let file = prime.join("context.json");
                let prime_meta = std::fs::symlink_metadata(&prime).ok()?;
                let file_meta = std::fs::symlink_metadata(&file).ok()?;
                (!prime_meta.file_type().is_symlink()
                    && file_meta.file_type().is_file()
                    && crate::platform::is_owned_by_current_user(&file_meta))
                .then_some(file)
            });
        let Some(file) = found else {
            return Ok(None);
        };
        let source = file.display().to_string();
        let pin = read_object(&file)?;
        match pin.get("team_id") {
            Some(Value::Null | Value::String(_)) => {
                let team_id = string_field(&pin, "team_id");
                let name = team_id
                    .as_ref()
                    .and_then(|_| string_field(&pin, "team_name"));
                return Ok(Some(PrimeDirectoryTeam {
                    team_id,
                    name,
                    source,
                }));
            }
            Some(Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_)) => {
                return Err(format!("{source}: team_id must be a string or null"));
            }
            None => {}
        }
        match pin.get("context") {
            None | Some(Value::Null) => return Ok(None),
            Some(Value::String(context)) => (context.clone(), source),
            Some(Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_)) => {
                return Err(format!("{source}: context must be a string or null"));
            }
        }
    };
    let valid_name = !context.is_empty()
        && context
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if !valid_name {
        return Err(format!("{source}: invalid Prime context name {context:?}"));
    }
    if context.eq_ignore_ascii_case("production") {
        return Ok(Some(PrimeDirectoryTeam {
            team_id: None,
            name: None,
            source,
        }));
    }
    let environment = read_object(
        &home
            .join(".prime")
            .join("environments")
            .join(format!("{context}.json")),
    )
    .map_err(|error| format!("Prime context '{context}' from {source}: {error}"))?;
    if normalize_base_url(string_field(&environment, "base_url").as_deref())
        != DEFAULT_PRIME_API_BASE_URL
    {
        return Ok(None);
    }
    Ok(Some(PrimeDirectoryTeam {
        team_id: string_field(&environment, "team_id"),
        name: string_field(&environment, "team_name"),
        source,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// A home with the global `.prime` (saved contexts `customer` on
    /// production and `dev` off it) and a repo at `home/code/repo`.
    fn layout() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let root = tempfile::tempdir().unwrap();
        // Canonical, so the expected pin paths match the walk's.
        let home = std::fs::canonicalize(root.path()).unwrap().join("home");
        let environments = home.join(".prime/environments");
        std::fs::create_dir_all(&environments).unwrap();
        std::fs::create_dir_all(home.join("code/repo/src")).unwrap();
        std::fs::write(
            environments.join("customer.json"),
            r#"{"base_url": "https://api.primeintellect.ai/api/v1", "team_id": "customer-team", "team_name": "Customer"}"#,
        )
        .unwrap();
        std::fs::write(
            environments.join("dev.json"),
            r#"{"base_url": "http://localhost:8000", "team_id": "dev-team"}"#,
        )
        .unwrap();
        let repo = home.join("code/repo");
        (root, home, repo)
    }

    fn pin(directory: &Path, content: &str) {
        std::fs::create_dir_all(directory.join(".prime")).unwrap();
        std::fs::write(directory.join(".prime/context.json"), content).unwrap();
    }

    fn team(team_id: Option<&str>, name: Option<&str>, source: &str) -> PrimeDirectoryTeam {
        PrimeDirectoryTeam {
            team_id: team_id.map(str::to_string),
            name: name.map(str::to_string),
            source: source.to_string(),
        }
    }

    #[test]
    fn resolves_the_team_a_directory_selects() {
        /// (case, pin ("" = none), `PRIME_CONTEXT`, expected; Err = refused)
        type Case<'a> = (
            &'a str,
            &'a str,
            Option<&'a str>,
            Result<Option<PrimeDirectoryTeam>, ()>,
        );
        let (_root, home, repo) = layout();
        let file = repo.join(".prime/context.json").display().to_string();
        let file = file.as_str();
        let cases: [Case; 14] = [
            ("no pin", "", None, Ok(None)),
            (
                "team pin",
                r#"{"team_id": "t1", "team_name": "T1"}"#,
                None,
                Ok(Some(team(Some("t1"), Some("T1"), file))),
            ),
            (
                "personal pin",
                r#"{"team_id": null, "team_name": "stale"}"#,
                None,
                Ok(Some(team(None, None, file))),
            ),
            (
                "an empty team is personal",
                r#"{"team_id": ""}"#,
                None,
                Ok(Some(team(None, None, file))),
            ),
            (
                "production context",
                r#"{"context": "production"}"#,
                None,
                Ok(Some(team(None, None, file))),
            ),
            (
                "saved context",
                r#"{"context": "customer"}"#,
                None,
                Ok(Some(team(Some("customer-team"), Some("Customer"), file))),
            ),
            (
                "team over context",
                r#"{"context": "customer", "team_id": "t1"}"#,
                None,
                Ok(Some(team(Some("t1"), None, file))),
            ),
            ("null context", r#"{"context": null}"#, None, Ok(None)),
            (
                "non-production context",
                r#"{"context": "dev"}"#,
                None,
                Ok(None),
            ),
            (
                "PRIME_CONTEXT over the pin",
                r#"{"team_id": "t1"}"#,
                Some("customer"),
                Ok(Some(team(
                    Some("customer-team"),
                    Some("Customer"),
                    "PRIME_CONTEXT",
                ))),
            ),
            ("missing context", r#"{"context": "gone"}"#, None, Err(())),
            (
                "invalid context name",
                r#"{"context": "../x"}"#,
                None,
                Err(()),
            ),
            ("non-string team", r#"{"team_id": 7}"#, None, Err(())),
            ("malformed pin", "[]", None, Err(())),
        ];
        for (name, content, prime_context, expected) in cases {
            let _ = std::fs::remove_dir_all(repo.join(".prime"));
            if !content.is_empty() {
                pin(&repo, content);
            }
            let resolved =
                resolve_directory_team(&repo.join("src"), &home, prime_context).map_err(|_| ());
            assert_eq!(resolved, expected, "{name}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn skips_the_pins_the_cli_skips() {
        let (root, home, repo) = layout();
        // Home's own `.prime` is the global config, never a pin.
        pin(&home, r#"{"team_id": "home-team"}"#);
        assert_eq!(resolve_directory_team(&repo, &home, None), Ok(None));

        // A symlinked `.prime` or pin file could point anywhere.
        let elsewhere = root.path().join("elsewhere");
        pin(&elsewhere, r#"{"team_id": "linked"}"#);
        std::os::unix::fs::symlink(elsewhere.join(".prime"), repo.join(".prime")).unwrap();
        assert_eq!(resolve_directory_team(&repo, &home, None), Ok(None));
        std::fs::remove_file(repo.join(".prime")).unwrap();
        std::fs::create_dir(repo.join(".prime")).unwrap();
        std::os::unix::fs::symlink(
            elsewhere.join(".prime/context.json"),
            repo.join(".prime/context.json"),
        )
        .unwrap();
        assert_eq!(resolve_directory_team(&repo, &home, None), Ok(None));
    }
}
