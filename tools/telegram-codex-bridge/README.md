# Telegram Codex Bridge

This local bridge lets a private Telegram bot send prompts to Codex CLI in allowed local repos.

## Security first

If a bot token was pasted into any chat, revoke it in `@BotFather` and generate a new token. Put the new token only in `.env` on this PC.

## Setup

1. Copy `.env.example` to `.env`.
2. Fill `TELEGRAM_BOT_TOKEN` with a new token from `@BotFather`.
3. Set a long random `CLAIM_CODE`.
4. Adjust `WORKSPACE_ROOT` and `ALLOWED_REPOS`.
5. Run:

```powershell
.\run.ps1
```

6. In Telegram, send:

```text
/claim your-claim-code
```

Then use:

```text
/repos
/use AI_builder_project
/run Read this repo and find the fastest way to run tests.
```

Plain messages are also treated as Codex prompts after the bridge is claimed.

## Requirements

- Python 3.12+ available as `python`
- Codex CLI standalone available as `codex`
- This PC online and awake

If `codex` from terminal returns `Access is denied`, install the standalone Codex CLI and run `codex login` before starting this bridge.
