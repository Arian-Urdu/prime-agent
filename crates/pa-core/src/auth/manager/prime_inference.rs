//! The Prime Inference credential writes (moved with its concern): the
//! locked read/modify/write of the prime-inference credential, the api-key
//! store with its team selection, the team rebind, and the stored team
//! read (TS setPrimeInferenceApiKey / setPrimeInferenceTeamSelection /
//! getPrimeInferenceTeamSelection), and the team header those selections
//! and a prime CLI directory context feed. The methods stay inherent on
//! `AuthStorage`: the impl owns the private lock and reload machinery they
//! wrap.

use super::*;

impl AuthStorage {
    /// TS `updatePrimeInferenceCredential`: one locked read/modify/write of
    /// the prime-inference credential; an update returning `None` leaves
    /// the document untouched. `true` when the locked run completed (TS
    /// returns normally; a write failure throws, so the callers must not
    /// treat a failed run as applied).
    fn update_prime_inference_credential(
        &mut self,
        update: impl FnOnce(Option<AuthCredential>) -> Option<AuthCredential>,
    ) -> bool {
        if self.load_error.is_some() {
            return false;
        }
        let mut update = Some(update);
        let result = self.storage.with_lock(&mut |current| {
            let mut data = parse_storage_data(current.as_deref())?;
            let existing = data.credential(PRIME_INFERENCE_PROVIDER_ID);
            let Some(credential) =
                (update.take().expect("the lock runs the update once"))(existing)
            else {
                return Ok(((), None));
            };
            data.insert(PRIME_INFERENCE_PROVIDER_ID, &credential);
            // TS writes `primeTeam: null` explicitly for the personal
            // account (`{ ...credential, primeTeam: null }`); the
            // declarative serde skips a `None` field, so the prime write
            // restores the key. The generic `set` keeps the TS omit
            // shape for other providers' keys.
            if let Some(serde_json::Value::Object(map)) =
                data.0.get_mut(PRIME_INFERENCE_PROVIDER_ID)
            {
                map.entry("primeTeam")
                    .or_insert_with(|| serde_json::Value::Null);
            }
            let content = serde_json::to_string_pretty(&data.0)?;
            Ok(((), Some(content)))
        });
        if let Err(error) = result {
            self.errors.push(error.to_string());
            return false;
        }
        self.reload();
        true
    }

    /// TS `setPrimeInferenceApiKey`: store the key and its team selection.
    pub fn set_prime_inference_api_key(&mut self, api_key: &str, team: PrimeTeamAssignment) {
        let api_key = api_key.to_string();
        let applied = self.update_prime_inference_credential(|existing| {
            let prime_team = match team {
                PrimeTeamAssignment::Team(team) => Some(team),
                PrimeTeamAssignment::PersonalAccount => None,
                // TS `undefined`: keep the stored team on the same key.
                PrimeTeamAssignment::PreserveWhenKeyMatches => match existing {
                    Some(AuthCredential::ApiKey {
                        key,
                        prime_team: stored,
                    }) if key == api_key => stored,
                    _ => None,
                },
            };
            Some(AuthCredential::ApiKey {
                key: api_key,
                prime_team,
            })
        });
        // TS: the stale clear sits after the write and never runs on a
        // failed one — a failed replacement must not re-enable the
        // server-rejected credential.
        if applied {
            self.clear_stale_auth_source(PRIME_INFERENCE_PROVIDER_ID, AuthSource::Stored);
        }
    }

    /// TS `setPrimeInferenceTeamSelection`: rebind the stored key's team;
    /// `expected_api_key: None` skips the key check (TS `undefined`).
    pub fn set_prime_inference_team_selection(
        &mut self,
        team: Option<PrimeTeamCredential>,
        expected_api_key: Option<&str>,
    ) {
        self.update_prime_inference_credential(|existing| {
            let Some(AuthCredential::ApiKey { key, .. }) = existing else {
                return None;
            };
            if let Some(expected) = expected_api_key {
                if key != expected {
                    return None;
                }
            }
            Some(AuthCredential::ApiKey {
                key,
                prime_team: team,
            })
        });
    }

    /// TS `getPrimeInferenceTeamSelection`: the stored team selection, or
    /// [`StoredPrimeTeam::NotSelected`] when `PRIME_TEAM_ID` pins the team
    /// or no api-key credential is stored. Fleet divergence (P5): the stored
    /// primeTeam survives runtime and environment API-key overrides — TS
    /// returns `undefined` when those are the active source, which forced
    /// dogfood daemons to pin `PRIME_TEAM_ID` in the environment; the stored
    /// login's team is used with whichever key is active instead.
    pub fn get_prime_inference_team_selection(&self) -> StoredPrimeTeam {
        if self
            .env_credentials
            .prime_team_id()
            .and_then(|value| {
                let trimmed = value.trim().to_string();
                (!trimmed.is_empty()).then_some(trimmed)
            })
            .is_some()
        {
            return StoredPrimeTeam::NotSelected;
        }
        match self.data.credential(PRIME_INFERENCE_PROVIDER_ID) {
            Some(AuthCredential::ApiKey { prime_team, .. }) => match prime_team {
                Some(team) => StoredPrimeTeam::Team(team),
                None => StoredPrimeTeam::PersonalAccount,
            },
            _ => StoredPrimeTeam::NotSelected,
        }
    }

    /// Resolve the Prime Inference team from the prime CLI directory context
    /// (`.prime/context.json`, or `PRIME_CONTEXT`) of the session directory
    /// `cwd`, ahead of the stored team. The stored credential is untouched.
    #[must_use]
    pub fn with_project_dir(mut self, cwd: impl Into<std::path::PathBuf>) -> Self {
        self.project_dir = Some(cwd.into());
        self
    }

    /// The team the project directory's prime CLI directory context selects,
    /// if any. A malformed pin or missing context is logged and ignored.
    pub fn prime_directory_team(&self) -> Option<crate::auth::PrimeDirectoryTeam> {
        let cwd = self.project_dir.as_deref()?;
        let prime_dir = self.prime_dir.clone().or_else(|| {
            crate::auth::default_prime_cli_config_path()
                .parent()
                .map(std::path::Path::to_path_buf)
        })?;
        let prime_context = self.env_credentials.prime_context();
        match crate::auth::prime_directory::resolve_directory_team(
            cwd,
            &prime_dir,
            prime_context.as_deref(),
        ) {
            Ok(team) => team,
            Err(error) => {
                tracing::warn!(%error, "ignoring Prime directory context");
                None
            }
        }
    }

    /// Provider-scoped request headers (prime-inference team header only):
    /// `PRIME_TEAM_ID`, then the directory context, then the stored team.
    /// The stored primeTeam survives runtime and environment API-key
    /// overrides (fleet P5): an ambient `PRIME_API_KEY` supplies the key,
    /// never the team.
    pub fn get_provider_headers(
        &self,
        provider_id: &str,
    ) -> Option<std::collections::HashMap<String, String>> {
        if provider_id != PRIME_INFERENCE_PROVIDER_ID {
            return None;
        }
        let env_team = self
            .env_credentials
            .prime_team_id()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let team_id = match env_team {
            Some(team_id) => Some(team_id),
            None => match self.prime_directory_team() {
                Some(directory) => directory.team_id,
                None => match self.data.credential(provider_id) {
                    Some(AuthCredential::ApiKey { prime_team, .. }) => {
                        prime_team.map(|team| team.team_id)
                    }
                    _ => None,
                },
            },
        };
        team_id.map(|team_id| {
            std::collections::HashMap::from([("X-Prime-Team-ID".to_string(), team_id)])
        })
    }
}
