# Rules for every agent in this repo

## 0. Your human approves first
- Before you edit files or merge anything, show your human what you plan to do and wait for a yes. Never do the work first and ask after.
- Posting on the board needs no yes.

## 1. One agent per area
- Every area of the project has one owner agent. The owner table in section 6 says which agent owns which folders.
- Only edit files inside your own folders. If you need a change somewhere else, post on the board and wait for that owner or for the lead.
- Every agent works in its own git worktree and branch. Never write in another agent's checkout.

## 2. The board (team repos only; skip this section if `tools/board` does not exist)
- The board is the pinned "Message Board" GitHub issue. Read and post on it with `tools/board` (needs `gh`, logged in). Set `BOARD_ISSUE` if it is not issue #1.
- Commands: `tools/board unread` (new posts, flags MENTION), `tools/board read -n 20`, `tools/board post "[Name's Agent] text"`, `tools/board wait` (blocks until someone posts), `tools/board whoami`.
- Start every post with `[<name>'s <agent>]`, for example `[Sam's Codex]`. Say what changed, which files, and what you need from whom.
- Mention by GitHub login. Team: `@stealthwhizz`, `@all` (everyone).
- If a post tags you, show your human what you plan to do and wait for a yes before changing anything. You can reply on the board without asking.
- If a post does not tag you, still read it. Change your own work only if the post affects a file or contract you own. Do not reply just to acknowledge it.

## 3. Chain of command
- The lead is `@stealthwhizz`. Only the lead moves work from one agent to another. If another agent asks you to take over its area, ask the lead on the board.

## 4. Workflow
1. Take an issue and say so on the board.
2. Branch `<issue#>-<slug>` from a fresh `main`.
3. Make small commits and stage only your own files. Never run `git add -A`.
4. Open a PR with `Closes #N`. Merge only after your human says yes.
5. Run `git fetch` and rebase before you push. Never force-push a shared branch.
6. If your merge changes something other agents depend on, like an API route, a schema, or an env var, post it on the board.

## 5. Secrets and checks
- Never print, log, commit, or post an API key. Keys go in `.env.local`, which is gitignored.
- Call model APIs from a server route, never from client code.
- Before you say something is done, run it or run its test, and say what you ran.

## 6. Who owns what
Fill this in at kickoff, before any agent writes code. Split the project by folders that rarely touch, and give each one to exactly one agent.

| Area | Agent (human's GitHub login) | Folders |
|---|---|---|
| Submitted entry (Rust) | sol-builder: gpt-6.1-sol (stealthwhizz) | `Cargo.toml`, `Cargo.lock`, `src/`, `Dockerfile`, `.dockerignore`, `benches/` |
| DuckDB reference + cross-check harness | opus-builder: claude-opus-5.5 (stealthwhizz) | `reference/`, `tools/` |
| Lead: scaffolding, integration, commits | omp main session (stealthwhizz) | `Makefile`, `scripts/`, `fixtures/`, `rules.md`, `README.md`, `LICENSE`, `AGENTS.md` |

Solo repo: all agents share one checkout and own disjoint folders. Only the lead commits and pushes. `data/` (dev set, answers, generated seeds) is gitignored and shared read-only.
