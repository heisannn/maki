//! Slash commands typed into an ACP prompt. Clients list them from
//! `available_commands_update` and send them back as plain prompt text.

use std::iter;

use agent_client_protocol_schema::v1::{
    AvailableCommand, AvailableCommandInput, UnstructuredCommandInput,
};
use maki_agent::command::CustomCommand;

const COMMAND_PREFIX: char = '/';
const COMPACT: &str = "compact";
const COMPACT_DESCRIPTION: &str = "Summarize and compact conversation history";
const COMPACT_HINT: &str = "optional guidance on what to keep";
const ARGUMENTS_HINT: &str = "arguments";

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Compact(Option<String>),
    /// A custom command, already rendered into the prompt it stands for.
    Prompt(String),
}

pub fn available(custom: &[CustomCommand]) -> Vec<AvailableCommand> {
    iter::once(AvailableCommand::new(COMPACT, COMPACT_DESCRIPTION).input(hint(COMPACT_HINT)))
        .chain(custom.iter().map(|cmd| {
            let command = AvailableCommand::new(acp_name(cmd), cmd.description.clone());
            if cmd.has_args() {
                command.input(hint(ARGUMENTS_HINT))
            } else {
                command
            }
        }))
        .collect()
}

/// `None` for anything that is not a known command, so a message that merely
/// starts with a slash (a path, say) still reaches the model untouched.
pub fn parse(text: &str, custom: &[CustomCommand]) -> Option<Command> {
    let line = text.strip_prefix(COMMAND_PREFIX)?;
    let (name, args) = line.split_once(char::is_whitespace).unwrap_or((line, ""));
    let args = args.trim();
    if name == COMPACT {
        return Some(Command::Compact(
            (!args.is_empty()).then(|| args.to_owned()),
        ));
    }
    custom
        .iter()
        .find(|cmd| acp_name(cmd) == name)
        .map(|cmd| Command::Prompt(cmd.render(args)))
}

/// ACP command names carry no slash: the client adds it when it shows them.
fn acp_name(cmd: &CustomCommand) -> String {
    cmd.display_name()
        .trim_start_matches(COMMAND_PREFIX)
        .to_owned()
}

fn hint(text: &str) -> AvailableCommandInput {
    AvailableCommandInput::Unstructured(UnstructuredCommandInput::new(text))
}

#[cfg(test)]
mod tests {
    use maki_agent::command::CommandScope;
    use test_case::test_case;

    use super::*;

    const GUIDANCE: &str = "keep the API design";
    const REVIEW_BODY: &str = "Review $ARGUMENTS carefully";

    fn review() -> CustomCommand {
        CustomCommand {
            name: "review".into(),
            description: "Review code".into(),
            content: REVIEW_BODY.into(),
            scope: CommandScope::Project,
            accepts_args: true,
        }
    }

    #[test_case("/compact", Some(Command::Compact(None)) ; "bare_compact")]
    #[test_case("/compact   ", Some(Command::Compact(None)) ; "whitespace_only_args_are_empty")]
    #[test_case(&format!("/compact {GUIDANCE}"), Some(Command::Compact(Some(GUIDANCE.into()))) ; "compact_with_guidance")]
    #[test_case(&format!("/compact\n{GUIDANCE}"), Some(Command::Compact(Some(GUIDANCE.into()))) ; "guidance_on_next_line")]
    #[test_case("/project:review src/lib.rs", Some(Command::Prompt("Review src/lib.rs carefully".into())) ; "custom_command_renders_args")]
    #[test_case("/compacted", None ; "prefix_of_a_longer_word")]
    #[test_case("/usr/bin is missing", None ; "path_is_not_a_command")]
    #[test_case("compact", None ; "no_slash")]
    #[test_case("/user:review", None ; "wrong_scope")]
    fn parse_cases(text: &str, expected: Option<Command>) {
        assert_eq!(parse(text, &[review()]), expected);
    }

    #[test]
    fn available_lists_compact_then_custom_without_slash() {
        let json = serde_json::to_value(available(&[review()])).unwrap();
        assert_eq!(json[0]["name"], COMPACT);
        assert_eq!(json[0]["input"]["hint"], COMPACT_HINT);
        assert_eq!(json[1]["name"], "project:review");
        assert_eq!(json[1]["input"]["hint"], ARGUMENTS_HINT);
    }
}
