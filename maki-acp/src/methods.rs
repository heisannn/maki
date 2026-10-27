use std::iter;

use agent_client_protocol_schema::ProtocolVersion;
use agent_client_protocol_schema::v1::{
    AgentCapabilities, Implementation, InitializeResponse, LoadSessionResponse, McpCapabilities,
    NewSessionResponse, PromptCapabilities, SessionCapabilities, SessionConfigOption,
    SessionConfigOptionCategory, SessionConfigSelectOption, SessionListCapabilities, SessionMode,
    SessionModeId, SessionModeState,
};
use maki_providers::ThinkingConfig;
use maki_providers::model::Model;
use maki_storage::sessions::{StoredThinking, THINKING_ADAPTIVE};

const VERSION: &str = env!("CARGO_PKG_VERSION");

const MODE_BUILD: &str = "build";
const MODE_PLAN: &str = "plan";

pub const MODEL_CONFIG_ID: &str = "model";
pub const THINKING_CONFIG_ID: &str = "thinking";

pub fn initialize_response() -> InitializeResponse {
    InitializeResponse::new(ProtocolVersion::V1)
        .agent_capabilities(
            AgentCapabilities::new()
                .load_session(true)
                .prompt_capabilities(PromptCapabilities::new().image(true).embedded_context(true))
                .mcp_capabilities(McpCapabilities::new().http(true))
                .session_capabilities(
                    SessionCapabilities::new().list(SessionListCapabilities::new()),
                ),
        )
        .auth_methods(vec![])
        .agent_info(Implementation::new("maki", VERSION))
}

pub fn mode_state(current: &str) -> SessionModeState {
    SessionModeState::new(
        SessionModeId::from(current.to_string()),
        vec![
            SessionMode::new(SessionModeId::from(MODE_BUILD.to_string()), "Build"),
            SessionMode::new(SessionModeId::from(MODE_PLAN.to_string()), "Plan"),
        ],
    )
}

pub fn new_session_response(session_id: &str) -> NewSessionResponse {
    NewSessionResponse::new(session_id.to_string()).modes(mode_state(MODE_BUILD))
}

pub fn load_session_response() -> LoadSessionResponse {
    LoadSessionResponse::new().modes(mode_state(MODE_BUILD))
}

pub fn model_config_option(current: &str, specs: &[String]) -> SessionConfigOption {
    let mut options: Vec<SessionConfigSelectOption> = specs
        .iter()
        .map(|spec| SessionConfigSelectOption::new(spec.clone(), spec.clone()))
        .collect();
    if !specs.iter().any(|spec| spec == current) {
        options.insert(
            0,
            SessionConfigSelectOption::new(current.to_string(), current.to_string()),
        );
    }
    SessionConfigOption::select(MODEL_CONFIG_ID, "Model", current.to_string(), options)
        .category(SessionConfigOptionCategory::Model)
}

/// The model's own ladder, minus `adaptive`, with `current` as the model will
/// really run it. `None` for a model without thinking.
pub fn thinking_config_option(
    model: &Model,
    current: StoredThinking,
) -> Option<SessionConfigOption> {
    let current = StoredThinking::from(ThinkingConfig::from(current).clamped(model)).to_string();
    let mut levels: Vec<String> = model
        .thinking_options()
        .into_iter()
        .filter(|option| option.name != THINKING_ADAPTIVE)
        .map(|option| option.name.to_owned())
        .collect();
    if levels.is_empty() {
        return None;
    }
    if !levels.contains(&current) {
        levels.push(current.clone());
    }
    let options = levels
        .into_iter()
        .map(|level| SessionConfigSelectOption::new(level.clone(), level))
        .collect::<Vec<_>>();
    Some(
        SessionConfigOption::select(THINKING_CONFIG_ID, "Thinking", current, options)
            .category(SessionConfigOptionCategory::ThoughtLevel),
    )
}

pub fn config_options(
    model: &str,
    specs: &[String],
    thinking: StoredThinking,
) -> Vec<SessionConfigOption> {
    let thinking = Model::from_spec(model)
        .ok()
        .and_then(|model| thinking_config_option(&model, thinking));
    iter::once(model_config_option(model, specs))
        .chain(thinking)
        .collect()
}

pub fn mode_id_to_agent_mode(mode_id: &str) -> Option<maki_agent::AgentMode> {
    match mode_id {
        MODE_BUILD => Some(maki_agent::AgentMode::Build),
        MODE_PLAN => {
            let storage = maki_storage::StateDir::resolve().ok()?;
            let plan_path = maki_storage::plans::new_plan_path(&storage).ok()?;
            Some(maki_agent::AgentMode::Plan(plan_path))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use agent_client_protocol_schema::v1::{SessionConfigKind, SessionConfigSelectOptions};
    use maki_storage::sessions::Effort;
    use test_case::test_case;

    use super::*;

    const CLAUDE_CODE_SPEC: &str = "claude-code/claude-opus-5-5";
    const LADDER: [&str; 5] = ["low", "medium", "high", "xhigh", "max"];

    fn thinking_values(option: &SessionConfigOption) -> (String, Vec<String>) {
        let SessionConfigKind::Select(select) = &option.kind else {
            panic!("thinking is not a select");
        };
        let SessionConfigSelectOptions::Ungrouped(options) = &select.options else {
            panic!("thinking options are grouped");
        };
        let values = options.iter().map(|o| o.value.0.to_string()).collect();
        (select.current_value.0.to_string(), values)
    }

    #[test_case(StoredThinking::Off, "medium" ; "off_shows_as_the_default_rung")]
    #[test_case(StoredThinking::Effort { level: Effort::XHigh }, "xhigh" ; "a_rung_is_kept")]
    #[test_case(StoredThinking::Effort { level: Effort::Minimal }, "low" ; "minimal_shows_as_low")]
    fn thinking_follows_the_model_ladder(current: StoredThinking, shown: &str) {
        let model = Model::from_spec(CLAUDE_CODE_SPEC).unwrap();
        let option = thinking_config_option(&model, current).unwrap();
        assert_eq!(
            thinking_values(&option),
            (shown.to_owned(), LADDER.map(String::from).to_vec())
        );
    }
}
