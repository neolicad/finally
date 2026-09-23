# FinAlly — AI Trading Workstation

A Bloomberg-style trading workstation with live streaming prices, a simulated $10k portfolio, and an LLM chat assistant that can analyze positions and execute trades from natural language.

Built entirely by coding agents as a capstone for an agentic AI coding course.

## Features

- Live prices over SSE with green/red flash animations and sparklines
- Simulated portfolio: market orders, instant fills, heatmap, P&L chart, positions table
- AI chat that analyzes holdings and auto-executes trades and watchlist changes
- Dark, data-dense terminal UI

## Architecture

One Docker container, one port (8000):

- **Frontend**: Next.js (static export), TypeScript, Tailwind, Recharts
- **Backend**: FastAPI managed with `uv`, SSE streaming
- **Database**: SQLite, lazily initialized
- **AI**: LiteLLM → OpenRouter (Cerebras) with structured outputs
- **Market data**: built-in GBM simulator, or Massive API if a key is set

## Status

| Component | State |
|---|---|
| Market data (simulator, Massive client, price cache, SSE) | Done — see `planning/MARKET_DATA_SUMMARY.md` |
| Portfolio, watchlist, chat API | Planned |
| Frontend | Planned |
| Docker and start/stop scripts | Planned |

## Quick Start

Market data backend (available now):

```bash
cd backend
uv sync --extra dev
uv run --extra dev pytest
uv run market_data_demo.py
```

Full app (once complete):

```bash
cp .env.example .env   # add OPENROUTER_API_KEY
docker build -t finally .
docker run -v finally-data:/app/db -p 8000:8000 --env-file .env finally
```

Then open http://localhost:8000.

## Environment Variables

| Variable | Purpose |
|---|---|
| `OPENROUTER_API_KEY` | Required for AI chat |
| `MASSIVE_API_KEY` | Optional; real market data instead of the simulator |
| `LLM_MOCK` | `true` for deterministic mock LLM responses (tests) |

## Docs

Full specification: `planning/PLAN.md`.

## License

See `LICENSE`.
