//! Prime CLI directory contexts: the Prime team a working directory selects
//! through `PRIME_CONTEXT` or the nearest `.prime/context.json`, resolved the
//! way the prime CLI resolves it (`prime switch <team> --local`).

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use serde_json::Value;

use super::prime_inference::{normalize_base_url, string_field, DEFAULT_PRIME_API_BASE_URL};

/// The team a directory context selects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimeDirectoryTeam {
    /// `None` is the personal account.
    pub team_id: Option<String>,
    pub name: Option<String>,
    /// The pin file, or `PRIME_CONTEXT`.
    pub source: String,
}

/// The nearest `.prime/context.json` at or above `cwd`, stopping at `home`
/// (whose `.prime` is the global config) and skipping symlinks and files
/// owned by another user.
fn find_context_file(cwd: &Path, home: &Path) -> Option<PathBuf> {
    let home = std::fs::canonicalize(home).unwrap_or_else(|_| home.to_path_buf());
    let start = std::fs::canonicalize(cwd).unwrap_or_else(|_| cwd.to_path_buf());
    let uid = nix::unistd::Uid::current().as_raw();
    for directory in start.ancestors().take_while(|directory| *directory != home) {
        let prime = directory.join(".prime");
        let candidate = prime.join("context.json");
        let (Ok(prime_meta), Ok(meta)) = (
            std::fs::symlink_metadata(&prime),
            std::fs::symlink_metadata(&candidate),
        ) else {
            continue;
        };
        if !prime_meta.file_type().is_symlink() && meta.file_type().is_file() && meta.uid() == uid {
            return Some(candidate);
        }
    }
    None
}

fn read_object(path: &Path) -> Result<serde_json::Map<String, Value>, String> {
    let content =
        std::fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?;
    match serde_json::from_str(&content) {
        Ok(Value::Object(object)) => Ok(object),
        Ok(_) => Err(format!("{}: expected a JSON object", path.display())),
        Err(error) => Err(format!("{}: {error}", path.display())),
    }
}

/// The team `PRIME_CONTEXT` (`prime_context`) or the directory context of
/// `cwd` selects, or `None` when neither applies. A saved context that
/// targets a non-production API is ignored: Prime Inference is
/// production-only. Errors describe a malformed pin or missing context.
pub(crate) fn resolve_directory_team(
    cwd: &Path,
    prime_dir: &Path,
    prime_context: Option<&str>,
) -> Result<Option<PrimeDirectoryTeam>, String> {
    let explicit = prime_context
        .map(str::trim)
        .filter(|context| !context.is_empty());
    let (context, source) = if let Some(context) = explicit {
        (context.to_string(), "PRIME_CONTEXT".to_string())
    } else {
        let home = prime_dir.parent().unwrap_or(prime_dir);
        let Some(file) = find_context_file(cwd, home) else {
            return Ok(None);
        };
        let source = file.display().to_string();
        let pin = Value::Object(read_object(&file)?);
        match pin.get("team_id") {
            Some(Value::Null) => {
                return Ok(Some(PrimeDirectoryTeam {
                    team_id: None,
                    name: None,
                    source,
                }));
            }
            Some(Value::String(_)) => {
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
            Some(_) => return Err(format!("{source}: team_id must be a string or null")),
            None => {}
        }
        match pin.get("context") {
            None => return Ok(None),
            Some(Value::String(context)) => (context.clone(), source),
            Some(_) => return Err(format!("{source}: context must be a string")),
        }
    };
    let valid_name = !context.is_empty()
        && context
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if !valid_name {
        return Err(format!("invalid Prime context name (from {source})"));
    }
    if context.eq_ignore_ascii_case("production") {
        return Ok(Some(PrimeDirectoryTeam {
            team_id: None,
            name: None,
            source,
        }));
    }
    let environment = Value::Object(read_object(
        &prime_dir
            .join("environments")
            .join(format!("{context}.json")),
    )?);
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

    /// A home with a global `.prime` (saved contexts `customer` on
    /// production and `dev` elsewhere) and a repo at `home/code/repo/src`.
    fn layout() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        let environments = home.join(".prime").join("environments");
        std::fs::create_dir_all(&environments).unwrap();
        std::fs::create_dir_all(home.join("code/repo/src")).unwrap();
        std::fs::write(
            environments.join("customer.json"),
            r#"{"base_url": "https://api.primeintellect.ai", "team_id": "customer-team"}"#,
        )
        .unwrap();
        std::fs::write(
            environments.join("dev.json"),
            r#"{"base_url": "http://x", "team_id": "d"}"#,
        )
        .unwrap();
        (root, home.clone(), home.join("code/repo"))
    }

    fn pin(directory: &Path, content: &str) {
        std::fs::create_dir_all(directory.join(".prime")).unwrap();
        std::fs::write(directory.join(".prime/context.json"), content).unwrap();
    }

    #[test]
    fn resolves_the_team_a_directory_selects() {
        // (case, pin, PRIME_CONTEXT, expected team: None = no selection,
        // Some(None) = personal; Err = malformed/missing)
        type Expected = Result<Option<Option<&'static str>>, ()>;
        let cases: [(&str, &str, Option<&str>, Expected); 10] = [
            ("no pin", "", None, Ok(None)),
            (
                "team pin",
                r#"{"team_id": "t1"}"#,
                None,
                Ok(Some(Some("t1"))),
            ),
            ("personal pin", r#"{"team_id": null}"#, None, Ok(Some(None))),
            (
                "production context",
                r#"{"context": "production"}"#,
                None,
                Ok(Some(None)),
            ),
            (
                "saved context",
                r#"{"context": "customer"}"#,
                None,
                Ok(Some(Some("customer-team"))),
            ),
            (
                "team over context",
                r#"{"context": "customer", "team_id": "t1"}"#,
                None,
                Ok(Some(Some("t1"))),
            ),
            (
                "non-production context",
                r#"{"context": "dev"}"#,
                None,
                Ok(None),
            ),
            (
                "PRIME_CONTEXT wins",
                r#"{"team_id": "t1"}"#,
                Some("customer"),
                Ok(Some(Some("customer-team"))),
            ),
            ("missing context", r#"{"context": "gone"}"#, None, Err(())),
            ("malformed pin", "[]", None, Err(())),
        ];
        for (name, content, prime_context, expected) in cases {
            let (_root, home, repo) = layout();
            if !content.is_empty() {
                pin(&repo, content);
            }
            let resolved =
                resolve_directory_team(&repo.join("src"), &home.join(".prime"), prime_context)
                    .map(|team| team.map(|team| team.team_id))
                    .map_err(|_| ());
            let expected = expected.map(|team| team.map(|id| id.map(str::to_string)));
            assert_eq!(resolved, expected, "{name}");
        }
    }

    #[test]
    fn ignores_pins_the_cli_ignores() {
        let (root, home, repo) = layout();
        pin(&home, r#"{"team_id": "home-team"}"#);
        pin(&root.path().join("elsewhere"), r#"{"team_id": "linked"}"#);
        std::os::unix::fs::symlink(root.path().join("elsewhere/.prime"), repo.join(".prime"))
            .unwrap();
        assert_eq!(
            resolve_directory_team(&repo.join("src"), &home.join(".prime"), None),
            Ok(None)
        );
    }
}
