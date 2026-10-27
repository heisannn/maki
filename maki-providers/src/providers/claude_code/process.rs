use std::env;
use std::path::{Path, absolute};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use flume::Sender;
use futures_lite::future::{or, try_zip};
use futures_lite::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use serde_json::{Value, json};
use smol::process::{Command, Stdio};

use crate::providers::{Timeouts, next_sse_line};
use crate::{AgentError, ProviderEvent, StreamResponse};

use super::config_error;
use super::relay::Relay;

const CONFIG_DIR_ENV: &str = "MAKI_CLAUDE_CODE_CONFIG_DIR";
const ALLOW_CREDITS_ENV: &str = "MAKI_CLAUDE_CODE_ALLOW_USAGE_CREDITS";
const LOGIN_HINT: &str = "Claude Code requires a subscription login; run `claude auth login`";
const OVERRIDE_ERROR: &str = "Claude Code subscription provider refuses auth/backend overrides";
const OVERAGE_ERROR: &str = "Claude Code reported usage-credit billing; stopped further turns. Set MAKI_CLAUDE_CODE_ALLOW_USAGE_CREDITS=1 to allow it";
const EXIT_ERROR: &str = "Claude Code exited without a completed model response; check its version and `claude auth status`";
const AUTH_OVERRIDES: &[&str] = &[
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_FOUNDRY_API_KEY",
    "CLAUDE_CODE_BASE_URL",
    "CLAUDE_CODE_SIMPLE",
];
const BACKEND_OVERRIDES: &[&str] = &[
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_VERTEX",
    "CLAUDE_CODE_USE_FOUNDRY",
];
const CHILD_ENV: &[(&str, &str)] = &[
    ("ENABLE_TOOL_SEARCH", "false"),
    ("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1"),
    ("CLAUDE_CODE_MAX_RETRIES", "0"),
    ("DISABLE_AUTO_COMPACT", "1"),
    ("DISABLE_COMPACT", "1"),
    ("CLAUDE_CODE_TOTAL_TOKENS_REMINDER", "off"),
    ("DISABLE_TELEMETRY", "1"),
    ("DISABLE_ERROR_REPORTING", "1"),
];

pub(super) fn check_environment(
    env: impl Iterator<Item = (String, String)>,
) -> Result<(), AgentError> {
    let conflicts: Vec<String> = env
        .filter_map(|(key, value)| {
            let active = !matches!(
                value.to_ascii_lowercase().as_str(),
                "" | "0" | "false" | "no" | "off"
            );
            ((AUTH_OVERRIDES.contains(&key.as_str()) && !value.is_empty())
                || (BACKEND_OVERRIDES.contains(&key.as_str()) && active))
                .then_some(key)
        })
        .collect();
    if conflicts.is_empty() {
        Ok(())
    } else {
        Err(config_error(format!(
            "{OVERRIDE_ERROR}: {}",
            conflicts.join(", ")
        )))
    }
}

fn command(executable: &Path) -> Result<Command, AgentError> {
    let mut command = Command::new(executable);
    command
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stderr(Stdio::null());
    if let Some(directory) =
        env::var_os(CONFIG_DIR_ENV).or_else(|| env::var_os("CLAUDE_CONFIG_DIR"))
    {
        command.env("CLAUDE_CONFIG_DIR", absolute(directory)?);
    }
    Ok(command)
}

pub(super) async fn check_login(executable: &Path, timeout: Duration) -> Result<(), AgentError> {
    let output = or(
        async {
            command(executable)?
                .args(["auth", "status"])
                .output()
                .await
                .map_err(AgentError::from)
        },
        async {
            smol::Timer::after(timeout).await;
            Err(config_error("Claude Code auth status timed out"))
        },
    )
    .await?;
    let status: Value =
        serde_json::from_slice(&output.stdout).map_err(|_| config_error(LOGIN_HINT))?;
    validate_login(output.status.success(), &status)
}

fn validate_login(success: bool, status: &Value) -> Result<(), AgentError> {
    if success
        && status["loggedIn"] == true
        && matches!(
            status["authMethod"].as_str(),
            Some("claude.ai" | "oauth_token")
        )
        && status["subscriptionType"]
            .as_str()
            .is_some_and(|plan| !plan.is_empty())
    {
        Ok(())
    } else {
        Err(config_error(LOGIN_HINT))
    }
}

fn check_billing(event: &Value, allow_credits: bool) -> Result<(), AgentError> {
    if event["type"] == "rate_limit_event"
        && event["rate_limit_info"]["isUsingOverage"] == true
        && !allow_credits
    {
        Err(config_error(OVERAGE_ERROR))
    } else {
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    executable: &Path,
    model: &str,
    system: &str,
    body: Value,
    betas: Vec<&'static str>,
    event_tx: Sender<ProviderEvent>,
    timeouts: Timeouts,
) -> Result<StreamResponse, AgentError> {
    let directory = tempfile::tempdir()?;
    let system_path = directory.path().join("system.md");
    smol::fs::write(&system_path, system).await?;
    let relay = Relay::bind(timeouts).await?;
    let base_url = relay.base_url()?;
    let mut child = command(executable)?
        .args([
            "-p",
            "--model",
            model,
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--include-partial-messages",
            "--tools",
            "",
            "--disallowedTools",
            "*",
            "--setting-sources",
            "",
            "--strict-mcp-config",
            "--mcp-config",
            "{\"mcpServers\":{}}",
            "--disable-slash-commands",
            "--max-turns",
            "1",
            "--permission-mode",
            "dontAsk",
            "--no-session-persistence",
            "--settings",
            "{\"disableAllHooks\":true}",
            "--system-prompt-file",
        ])
        .arg(&system_path)
        .current_dir(directory.path())
        .envs(CHILD_ENV.iter().copied())
        .env_remove("CLAUDE_CODE_EXTRA_BODY")
        .env("ANTHROPIC_BASE_URL", base_url)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|error| config_error(format!("could not start Claude Code: {error}")))?;

    let mut stdin = child.stdin.take().ok_or_else(|| config_error(EXIT_ERROR))?;
    // History is put on the outgoing request by the relay, without CLI reminders
    // or tool recovery edits. The CLI only needs a frame that starts one query.
    let frame = json!({"type":"user","message":{"role":"user","content":"Continue."}});
    stdin.write_all(format!("{frame}\n").as_bytes()).await?;
    drop(stdin);

    let started = Arc::new(AtomicBool::new(false));
    let (response_tx, response_rx) = flume::bounded(1);
    let relay_task =
        smol::spawn(relay.serve(body, betas, event_tx, Arc::clone(&started), response_tx));
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| config_error(EXIT_ERROR))?;
    let response = try_zip(
        async {
            response_rx
                .recv_async()
                .await
                .map_err(|_| config_error(EXIT_ERROR))?
        },
        async {
            let mut lines = BufReader::new(stdout).lines();
            let mut deadline = Instant::now() + timeouts.stream;
            while let Some(line) = next_sse_line(&mut lines, &mut deadline, timeouts.stream).await?
            {
                let event: Value = serde_json::from_str(&line)?;
                check_billing(&event, env::var(ALLOW_CREDITS_ENV).as_deref() == Ok("1"))?;
                if event["type"] == "result" {
                    if started.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    return Err(config_error(format!(
                        "{EXIT_ERROR}: {}",
                        event["result"]
                            .as_str()
                            .unwrap_or("check `claude auth status`")
                    )));
                }
            }
            Err(config_error(EXIT_ERROR))
        },
    )
    .await;
    drop(relay_task);
    let _ = child.kill();
    let _ = child.status().await;
    response.map(|(response, ())| response)
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::fs::{self, Permissions};
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    #[cfg(unix)]
    use std::time::Duration;

    use serde_json::json;
    use test_case::test_case;

    #[cfg(unix)]
    use crate::providers::Timeouts;

    #[cfg(unix)]
    use super::{EXIT_ERROR, run};
    use super::{
        LOGIN_HINT, OVERAGE_ERROR, OVERRIDE_ERROR, check_billing, check_environment, validate_login,
    };

    #[cfg(unix)]
    const IO_TIMEOUT: Duration = Duration::from_secs(5);

    #[test_case("ANTHROPIC_API_KEY", "secret", false ; "api_key")]
    #[test_case("ANTHROPIC_BASE_URL", "http://localhost", false ; "proxy")]
    #[test_case("CLAUDE_CODE_USE_BEDROCK", "1", false ; "bedrock")]
    #[test_case("CLAUDE_CODE_USE_VERTEX", "false", true ; "disabled_backend")]
    #[test_case("CLAUDE_CODE_USE_FOUNDRY", "0", true ; "disabled_foundry")]
    #[test_case("ANTHROPIC_API_KEY", "", true ; "empty_key")]
    fn environment_overrides(key: &str, value: &str, allowed: bool) {
        let result = check_environment([(key.to_owned(), value.to_owned())].into_iter());
        if allowed {
            result.unwrap();
        } else {
            assert!(result.unwrap_err().to_string().starts_with(OVERRIDE_ERROR));
        }
    }

    #[test_case(true, true, "claude.ai", Some("max"), true ; "subscription")]
    #[test_case(true, true, "api_key", None, false ; "api_key")]
    #[test_case(true, true, "api_key_helper", Some("max"), false ; "helper")]
    #[test_case(true, false, "claude.ai", Some("max"), false ; "logged_out")]
    #[test_case(false, true, "claude.ai", Some("max"), false ; "failed_status")]
    #[test_case(true, true, "claude.ai", None, false ; "no_plan")]
    fn subscription_auth(
        success: bool,
        logged_in: bool,
        method: &str,
        plan: Option<&str>,
        allowed: bool,
    ) {
        let status = json!({"loggedIn":logged_in,"authMethod":method,"subscriptionType":plan});
        let result = validate_login(success, &status);
        if allowed {
            result.unwrap();
        } else {
            assert_eq!(result.unwrap_err().to_string(), LOGIN_HINT);
        }
    }

    #[test_case(false ; "overage_requires_opt_in")]
    #[test_case(true ; "explicit_overage_opt_in")]
    fn usage_credit_billing(allow_credits: bool) {
        let event = json!({"type":"rate_limit_event","rate_limit_info":{"isUsingOverage":true}});
        let result = check_billing(&event, allow_credits);
        if allow_credits {
            result.unwrap();
        } else {
            assert_eq!(result.unwrap_err().to_string(), OVERAGE_ERROR);
        }
    }

    #[cfg(unix)]
    #[test_case(r#"{"type":"result","result":"fixture failure"}"#, EXIT_ERROR ; "cli_fails_before_request")]
    fn cli_failure_does_not_wait_for_relay(event: &str, expected_error: &str) {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("fixture-claude");
            fs::write(
                &path,
                format!("#!/bin/sh\nIFS= read -r request\nprintf '%s\\n' '{event}'\n"),
            )
            .unwrap();
            fs::set_permissions(&path, Permissions::from_mode(0o700)).unwrap();
            let (event_tx, _events) = flume::unbounded();
            let timeouts = Timeouts {
                stream: IO_TIMEOUT,
                ..Timeouts::default()
            };
            let result = run(
                &path,
                "fixture",
                "fixture",
                json!({}),
                vec![],
                event_tx,
                timeouts,
            )
            .await;
            assert!(result.unwrap_err().to_string().starts_with(expected_error));
        });
    }
}
