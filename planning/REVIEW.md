# Review of Changes Since Last Commit (874b955)

## Changes reviewed

| File | Change |
|---|---|
| `README.md` | Rewritten (shorter; adds Status section; drops Project Structure) |
| `.claude/settings.json` | `playwright` plugin replaced by `independent-reviewer@neolicad-tools` |
| `.claude/commands/doc-review.md` | Deleted |
| `.claude-plugin/marketplace.json` | New: local marketplace `neolicad-tools` |
| `independent-reviewer/hooks/hooks.json` | New: Stop hook that requests this review |

## Findings

### Fixed during review

1. **README quick start was wrong.** `uv sync` / `uv run pytest` do not install pytest, because dev tools are the optional extra `dev` in `backend/pyproject.toml`. Changed to `uv sync --extra dev` and `uv run --extra dev pytest`, matching `backend/CLAUDE.md`. `backend/README.md` says `uv sync --dev`, which is inconsistent with the extra-based setup and should be checked separately.

### Open

2. **`playwright` plugin removed from settings.** `.claude/settings.json` no longer enables it. PLAN.md section 12 calls for Playwright E2E tests, but those run in a separate container, so this is likely fine. Confirm it was intentional.
3. **`doc-review.md` command deleted.** No other file references it, but anyone using `/doc-review` loses it. Confirm it is intended, possibly superseded by the new plugin.
4. **Stop hook fires on every stop.** `hooks.json` uses a bare prompt hook with no condition, so every session end demands a fresh `REVIEW.md`, even when nothing changed. It also overwrites the previous review. Consider scoping the prompt (for example, skip if no diff) or appending rather than overwriting.
5. **README references files that do not exist yet:** `.env.example`, `Dockerfile`, and `scripts/`. These are labelled "once complete" in the README, which is accurate for now. Update the README as each lands.
6. **README dropped the Project Structure tree.** This is acceptable for conciseness, but the tree helped orient readers. `planning/PLAN.md` section 4 still holds it.
7. **Not verified:** the README commands were checked against the docs and `pyproject.toml`, not executed. `uv run --extra dev pytest` was not run.

## Verdict

The README is accurate after the fix. The plugin and settings changes are small and coherent. Items 2 to 4 need the author's confirmation.
