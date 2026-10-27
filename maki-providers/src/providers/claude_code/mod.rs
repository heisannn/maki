mod process;
mod relay;

use std::env;
use std::path::{Path, PathBuf, absolute};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use flume::Sender;
use maki_config::providers::ProvidersConfig;
use maki_storage::id::SessionRef;
use serde_json::Value;
use tracing::debug;

use crate::model::{Model, ModelFamily, ModelInfo};
use crate::provider::{BoxFuture, Provider};
use crate::spec::{
    AuthDoc, Build, CatalogDoc, GeneratedDocs, Local, NO_CURATED_MODELS, ProviderSpec, api_decides,
};
use crate::{AgentError, Effort, Message, ProviderEvent, RequestOptions, StreamResponse};

use super::Timeouts;
use super::anthropic::{self, shared};

const SLUG: &str = "claude-code";
const DISPLAY_NAME: &str = "Claude Code (experimental)";
const COMMAND_ENV: &str = "MAKI_CLAUDE_CODE_COMMAND";
const DEFAULT_COMMAND: &str = if cfg!(windows) {
    "claude.exe"
} else {
    "claude"
};
const MODELS: &[&str] = &[
    "claude-opus-5-5",
    "claude-fable-5-1",
    "claude-sonnet-5-5",
    "claude-haiku-5-5",
];
const EFFORT_LADDER: &[Effort] = &[
    Effort::Low,
    Effort::Medium,
    Effort::High,
    Effort::XHigh,
    Effort::Max,
];
const LOGIN_CHECK_TTL: Duration = Duration::from_secs(5 * 60);
const INSTALL_HINT: &str =
    "Claude Code is not installed; install it or set MAKI_CLAUDE_CODE_COMMAND to its executable";
const AUTH_NOTE: &str = "Run `claude auth login` with a Claude subscription. Maki uses the official CLI and leaves credential storage and refresh to Claude Code.";
const USAGE_NOTE: &str = r#"Requires Claude Code on `PATH`. Choose a model with `maki --model claude-code/claude-sonnet-5-5` after `claude auth login`. Set `MAKI_CLAUDE_CODE_COMMAND` to use another executable and `MAKI_CLAUDE_CODE_CONFIG_DIR` to use another Claude Code configuration directory.

Maki owns conversation history, permissions and tool execution. Each model turn starts a CLI process with its tools disabled and a private loopback relay that permits one upstream generation. Requests use Claude Code's authentication. Maki does not store its tokens.

This integration is experimental and depends on Claude Code's wire format. Plan access, usage limits and billing are controlled by Anthropic. Maki stops if Claude Code reports usage-credit overage. Set `MAKI_CLAUDE_CODE_ALLOW_USAGE_CREDITS=1` to allow further turns charged to usage credits. The turn that reports overage has already been sent.

API-key and alternate-backend environment overrides are rejected to avoid silently changing billing. `CLAUDE_CODE_BASE_URL` and provider endpoint/header overrides are unsupported. Each turn replays Maki's history. CLI process reuse is not implemented."#;

pub(crate) const SPEC: ProviderSpec = ProviderSpec {
    slug: SLUG,
    display_name: DISPLAY_NAME,
    api_key_env: "",
    family: ModelFamily::Claude,
    supports_thinking: true,
    supports_deferred_tools: true,
    adaptive_effort: api_decides,
    thinking_fields_fallback: |_| None,
    effort_ladder: Some(EFFORT_LADDER),
    accepts_arbitrary_models: false,
    fallback_max_output: Some(64_000),
    fallback_context_window: shared::LONG_CONTEXT_WINDOW,
    models_toml: NO_CURATED_MODELS,
    pricing_schedule: None,
    build: Build::Local(Local {
        new: create,
        models_from: anthropic::SLUG,
        models: MODELS,
    }),
    aperture: None,
    login: None,
    docs: GeneratedDocs {
        api_urls: &[],
        features: Some("Claude Code CLI transport, prompt caching, thinking and Maki tools"),
        auth: AuthDoc::Custom(AUTH_NOTE),
        catalog: CatalogDoc::Discovered(
            "The picker lists Claude Opus 5.5, Fable 5.1, Sonnet 5.5 and Haiku 5.5 from the Anthropic provider's table, billed at $0 with the list price shown as a reference. Thinking takes the effort levels `low`, `medium`, `high`, `xhigh` and `max`.",
        ),
        trailing_notes: &[USAGE_NOTE],
    },
};

struct ClaudeCode {
    command: PathBuf,
    timeouts: Timeouts,
    login_verified_at: Mutex<Option<Instant>>,
}

impl ClaudeCode {
    /// `claude auth status` costs a CLI start, so a recent success is trusted.
    /// A login that lapses inside the window still fails the turn upstream.
    async fn check_login(&self) -> Result<(), AgentError> {
        let fresh = self
            .login_verified_at
            .lock()
            .unwrap()
            .is_some_and(|at| at.elapsed() < LOGIN_CHECK_TTL);
        if fresh {
            return Ok(());
        }
        process::check_login(&self.command, self.timeouts.connect).await?;
        *self.login_verified_at.lock().unwrap() = Some(Instant::now());
        Ok(())
    }
}

fn config_error(message: impl Into<String>) -> AgentError {
    AgentError::Config {
        message: message.into(),
    }
}

fn executable() -> Result<PathBuf, AgentError> {
    let command = env::var_os(COMMAND_ENV).unwrap_or_else(|| DEFAULT_COMMAND.into());
    let path = PathBuf::from(&command);
    if path.components().count() > 1 || path.is_absolute() {
        return is_executable(&path)
            .then_some(path)
            .ok_or_else(|| config_error(INSTALL_HINT))
            .and_then(|path| absolute(path).map_err(AgentError::from));
    }
    env::var_os("PATH")
        .into_iter()
        .flat_map(|paths| env::split_paths(&paths).collect::<Vec<_>>())
        .map(|directory| directory.join(&command))
        .find(|path| is_executable(path))
        .ok_or_else(|| config_error(INSTALL_HINT))
        .and_then(|path| absolute(path).map_err(AgentError::from))
}

fn is_executable(path: &Path) -> bool {
    path.metadata().is_ok_and(|metadata| {
        if !metadata.is_file() {
            return false;
        }
        #[cfg(unix)]
        {
            metadata.permissions().mode() & 0o111 != 0
        }
        #[cfg(not(unix))]
        {
            true
        }
    })
}

fn create(timeouts: Timeouts) -> Result<Box<dyn Provider>, AgentError> {
    process::check_environment(env::vars())?;
    if ProvidersConfig::load_or_default()
        .get(SLUG)
        .is_some_and(|config| config.base_url.is_some() || !config.headers.is_empty())
    {
        return Err(config_error(
            "Claude Code does not support provider endpoint/header overrides",
        ));
    }
    Ok(Box::new(ClaudeCode {
        command: executable()?,
        timeouts,
        login_verified_at: Mutex::new(None),
    }))
}

impl Provider for ClaudeCode {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        tools: &'a Value,
        event_tx: &'a Sender<ProviderEvent>,
        opts: RequestOptions,
        _session_id: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            process::check_environment(env::vars())?;
            self.check_login().await?;
            let body = shared::build_request_body_with_system(
                model,
                messages,
                &[],
                tools,
                opts.thinking,
                None,
            );
            let mut betas = vec![shared::LONG_CONTEXT_BETA];
            if shared::has_deferred_tools(tools) {
                betas.push(shared::BETA_DEFERRED_TOOLS);
            }
            debug!(provider = SLUG, model = %model.id, num_messages = messages.len(), thinking = ?opts.thinking, ?betas, "starting Claude Code model turn");
            process::run(
                &self.command,
                shared::strip_long_context(&model.id),
                system,
                body,
                betas,
                event_tx.clone(),
                self.timeouts,
            )
            .await
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async {
            self.check_login().await?;
            Ok(SPEC
                .models()
                .iter()
                .flat_map(|entry| {
                    entry.prefixes.iter().map(|id| ModelInfo {
                        id: id.clone(),
                        context_window: Some(shared::LONG_CONTEXT_WINDOW),
                        max_output_tokens: entry.max_output_tokens,
                        pricing: entry.pricing.clone(),
                        supports_thinking: Some(true),
                        supports_vision: entry.supports_vision,
                        tier: Some(entry.tier),
                        ..ModelInfo::default()
                    })
                })
                .collect())
        })
    }

    fn adjust_model(&self, model: &mut Model) {
        model.subsidised_by = Some(Arc::from("Claude Code"));
        model.context_window = shared::LONG_CONTEXT_WINDOW;
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;
    use crate::ThinkingConfig;

    const OPUS_SPEC: &str = "claude-code/claude-opus-5-5";

    #[test]
    fn picker_lists_only_the_served_models() {
        let ids: Vec<&str> = SPEC
            .models()
            .iter()
            .map(|entry| entry.prefixes[0].as_str())
            .collect();
        assert_eq!(ids, MODELS);
    }

    #[test]
    fn thinking_options_are_the_effort_ladder() {
        let model = Model::from_spec(OPUS_SPEC).unwrap();
        let names: Vec<&str> = model
            .thinking_options()
            .iter()
            .map(|option| option.name)
            .collect();
        let expected: Vec<&str> = EFFORT_LADDER.iter().map(|level| level.as_str()).collect();
        assert_eq!(names, expected);
    }

    #[test_case(ThinkingConfig::Off, Effort::Medium ; "off_runs_the_default")]
    #[test_case(ThinkingConfig::Adaptive, Effort::Medium ; "adaptive_runs_the_default")]
    #[test_case(ThinkingConfig::Effort(Effort::Minimal), Effort::Low ; "minimal_snaps_up")]
    #[test_case(ThinkingConfig::Effort(Effort::XHigh), Effort::XHigh ; "xhigh_is_kept")]
    #[test_case(ThinkingConfig::Budget(u32::MAX), Effort::Max ; "budget_maps_to_a_level")]
    fn thinking_clamps_onto_the_ladder(config: ThinkingConfig, expected: Effort) {
        let model = Model::from_spec(OPUS_SPEC).unwrap();
        assert_eq!(config.clamped(&model), ThinkingConfig::Effort(expected));
    }
}
