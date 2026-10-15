# Setting up market-anomaly-detection with Claude Code

## 1. Init

```bash
uv sync
uv run pytest -q
```

## 2. What Claude Code loads

| File | Role |
| --- | --- |
| `CLAUDE.md` | Every session: how to work with you (only what you ask), commands, research rules |
| `.claude/settings.json` | Pre-approves tests, Python, ruff, read-only git; blocks `.env` and edits to `data/raw` |
| `.claude/rules/research-hygiene.md` | Anti-leakage rules, loaded only when Claude touches `src` or `notebooks` |
| `.claude/agents/leakage-reviewer.md` | Reviewer, runs only when you ask: "use the leakage-reviewer on the diff" |
| `.claude/skills/journal` | `/journal`: appends an entry to `JOURNAL.md`, only when you type it |

## 3. The loop

1. Ask for one precise thing. Plan mode (`Shift+Tab`) for multi-file work.
2. Check the evidence it shows (counts, figure path) before asking for the next thing.
3. Optional: "use the leakage-reviewer on the diff", then `/journal`, then commit.
4. `/clear` before an unrelated task.

## 4. Habits

- `Esc` stops Claude; `Esc Esc` or `/rewind` goes back to a checkpoint.
- After two failed corrections, `/clear` and rewrite the prompt.
- When Claude repeats a mistake twice, add one line to `CLAUDE.md`. Delete lines that never matter.
- Personal tweaks go in `CLAUDE.local.md` or `.claude` (gitignored).

Reference: https://code.claude.com/docs/en/best-practices
