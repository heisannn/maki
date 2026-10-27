+++
title = "ACP"
weight = 22
[extra]
group = "Guides"
+++

# ACP (Agent Client Protocol)

Run Maki inside your editor. `maki acp` starts an [ACP](https://agentclientprotocol.com/) server over stdio, so any ACP-capable editor (like [Zed](https://zed.dev/)) can drive Maki as its coding agent.

```bash
maki acp
```

## Zed setup

Add Maki as a custom agent in Zed's `settings.json`:

```json
"agent_servers": {
  "Maki": {
    "default_config_options": {
      "model": "deepseek/deepseek-flash"
    },
    "type": "custom",
    "command": "maki",
    "args": ["acp"],
    "env": {}
  }
}
```

The `model` value is a `provider/model-id` spec, same format as `maki --model`.

## What works

- **Sessions persist.** Loading a session replays the full conversation in the editor, so you can resume where you left off.
- **Resume picker.** Editors can list this project's stored sessions, newest first, to resume an old transcript instead of starting a blank one.
- **Model switching.** Pick a model from the editor's dropdown, mid-session. All configured providers show up. Providers that list their models over the wire (OpenRouter and friends) are discovered in the background, so the dropdown keeps filling up for a moment after the session starts, one provider at a time.
- **Thinking.** Pick the thinking level (off, or minimal through max) from the editor, mid-session. It starts at your `always_thinking` config, or off.
- **Modes.** Switch between build (full access) and plan (plan-file writes only) from the editor.
- **Slash commands.** `/compact` (with optional guidance) and your [custom commands](/docs/commands/) show up in the editor's slash menu. Custom commands are named `project:<name>` or `user:<name>`. Other TUI commands are not available over ACP.
- **Permissions.** Tool permission prompts appear in the editor: allow or reject, once or always.
- **Questions.** The `question` tool becomes a native form in the editor (ACP elicitation). If the client does not support elicitation, the tool is dropped and the model asks in plain text.
- **Live tool calls.** Tool output streams into the editor while the tool runs, so a long `bash` command shows progress before it finishes. A sub-agent appears as its single `task` call. Calls inside `batch` and `code_execution` appear in the parent call's output.
- **Plan.** Each `todo_write` updates the editor's plan view. Cancelled items are left out, since ACP has no cancelled status.
- **Images and context.** Prompts can include images and editor-attached files.

Authentication, providers, and permissions come from your normal Maki config. Set up [providers](/docs/providers/) first and ACP sessions just work.

The editor picks each session's working directory, and that folder's own
[trust](/docs/folder-trust/) decides whether its `.maki` config loads. ACP never
asks, so trust a project with `maki trust add` in it, or start the server with
`maki --trust acp`.

```bash
maki acp
maki acp -m anthropic/claude-sonnet-4-6
maki acp --yolo
maki --no-jit acp
```

`maki acp` only takes `-m` / `--model` and `--yolo` as subcommand flags. Global flags like `--no-jit` must come before the subcommand (`maki --no-jit acp`, not `maki acp --no-jit`).

Plan mode in ACP uses the same state-directory plan files as the TUI (`…/plans/<slug>.md`), not the SDK's `./plan.md`.
