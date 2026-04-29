# Crypto Futures Signal Engine

**24/7 automated crypto futures signal engine** — built with AI-assisted development. Monitors 50+ Binance Futures pairs, applies multi-factor grading, manages a state machine with hysteresis-protected transitions, and delivers Telegram execution alerts.

## What It Does

```
Every 5 minutes (GitHub Actions CI/CD)
    │
    ├── Fetch live market data: Binance Futures
    ├── Calculate indicators: EMA 20/50/200, RSI 14, ATR 14, CVD
    ├── Apply grading: A+ / A / B / C / NO_TRADE
    ├── Check state machine transitions
    └── Deliver Telegram alerts (zero AI in delivery path)
```

## Architecture

```
openclaw_engine/          # Modular extractions
├── binance/              # Exchange API clients
├── delivery/             # Event priority & sorting
├── helpers/              # Time, file, HTTP utilities
├── indicators/           # EMA, RSI, ATR, CVD
├── projections/          # Notification lane mapping
├── renderer/             # Message formatting
├── scoring/              # Grading & execution filter
├── setup/                # Actionable setup planner
└── state/                # Pure state query functions

scripts/
└── public_market_data.py  # 622KB main engine (~11,500 lines)
```

## Technical Highlights

| Feature | Implementation |
|---------|---------------|
| **State Machine** | 6 lifecycle states × 6 runtime states, hysteresis-protected readiness |
| **Grading** | Multi-factor: EMA alignment, RSI zone, ATR channel, volume profile |
| **Execution Filter** | CVD divergence, crowding detection, risk flag overlay |
| **Live Monitoring** | WebSocket-based 5m candle streaming with starvation breaker |
| **Delivery** | Deterministic renderer → direct Telegram (zero AI reformatting) |
| **CI/CD** | GitHub Actions: automated scan + commit every 5 minutes |

## AI-Assisted Development

This project was built **iteratively over 7+ weeks** using AI pair programming:
- Phase 1: Characterization tests (10 files, ~2580 lines)
- Phase 2: Modular extraction (8 subpackages, zero behavioral changes)
- Ongoing: Production reliability hardening

> "Boil the lake" — completeness is near-free with AI. "Holy shit that's done" is the standard.

## Quick Start

```bash
# Scan top 20 pairs
python3 scripts/public_market_data.py scan --top 5 --universe 20

# Watchlist refresh
python3 scripts/public_market_data.py automation-cycle --kind watchlist --output-mode compact --top 5

# System status
python3 scripts/public_market_data.py state-status
```

## CI/CD Pipeline

The `main.yml` workflow runs every 5 minutes:
1. Checkout code
2. Install Python + dependencies
3. Run watchlist scan
4. Generate summary
5. Commit results automatically

**Live commits:** https://github.com/alkindivv/crypto-futures-signal-engine/commits/master

## For Xiaomi MiMo 100T

This project demonstrates:
- ✅ **Production-grade automation** — runs 24/7 on a VPS
- ✅ **Real-time data processing** — WebSocket-based price monitoring
- ✅ **CI/CD pipeline** — automated scans + commits every 5 minutes
- ✅ **AI-assisted development** — modular refactoring with 212 passing tests
- ✅ **Clean architecture** — separated concerns (indicators, scoring, state, delivery)

**Repository:** https://github.com/alkindivv/crypto-futures-signal-engine
