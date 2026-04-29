---
name: crypto-futures-depth-analysis
description: Deep Binance Futures and altcoin intraday analysis using public exchange APIs, WebSocket stream plans, Bybit cross-venue confirmation, CoinGecko market metadata, news RSS, and macro event context. Use when the user asks for Binance Futures scans, best long or short setups, deep single-coin analysis, signal validation, execution-quality checks, or when the assistant needs to proactively gather public market data without waiting for extra permission.
---

# Crypto Futures Depth Analysis

Use this skill to collect and structure public market data for Binance Futures analysis before forming a signal.

## Core workflow

1. Define the mode:
   - market scan
   - single-symbol analysis
   - live follow-up
   - data architecture / connector work
2. Use `scripts/public_market_data.py` for deterministic public-only pulls.
3. Build the evidence stack in this order:
   - Binance Futures REST snapshot
   - Binance Futures WebSocket plan for live tracking
   - Bybit confirmation
   - CoinGecko metadata and market-cap context
   - news RSS context
   - token unlock and exchange announcement context
   - macro calendar and macro headline context
   - execution filter using spread, depth, tape quality, crowding, and stored trade feedback
   - Massive grouped daily plus prev bar context
   - Massive custom bars for finalists only
   - TradingView TA and multi-timeframe confirmation for finalists only
   - LunarCrush social and sentiment cross-check for finalists only
4. Prioritize direct market evidence over commentary.
5. Downgrade or reject setups when execution quality is weak, catalyst risk is high, or the move depends on chasing price.

## Required data layers

### 1. Binance Futures, primary venue
Use Binance public REST and WebSocket data as the primary source for:
- OHLCV
- mark price
- funding
- open interest
- order book
- trade flow
- liquidation events

### 2. Bybit, confirmation venue
Use Bybit public data to confirm or challenge Binance structure:
- last price
- funding
- open interest
- kline alignment
- order book context

### 3. CoinGecko, market metadata
Use CoinGecko for:
- market-cap rank
- FDV
- circulating supply
- sector and narrative context
- trending context when useful

### 4. News and event layer
Use public RSS or public pages for:
- project-specific headlines
- exploit or legal headlines
- ETF or listing headlines
- event-driven invalidators

### 5. Macro layer
Use public macro sources to flag event risk:
- CPI
- PPI
- FOMC or Fed-related events
- other high-impact releases relevant to crypto beta

## Scripts and references

### scripts/public_market_data.py
Use this script when you need a reusable connector or scan engine.

Useful commands:

```bash
python3 scripts/public_market_data.py endpoint-map
python3 scripts/public_market_data.py provider-status
python3 scripts/public_market_data.py snapshot HYPEUSDT
python3 scripts/public_market_data.py snapshot HYPEUSDT --with-tradingview
python3 scripts/public_market_data.py scan --top 5 --enrich-top 2 --tv-top 2
python3 scripts/public_market_data.py ws-plan HYPEUSDT
python3 scripts/public_market_data.py event-layer HYPEUSDT
python3 scripts/public_market_data.py cmc-quotes BTC ETH SOL
python3 scripts/public_market_data.py nansen-holdings --chains ethereum --per-page 5
python3 scripts/public_market_data.py provider-enrichment HYPEUSDT
python3 scripts/public_market_data.py feedback-status
python3 scripts/public_market_data.py feedback-status LINKUSDT
python3 scripts/public_market_data.py feedback-log LINKUSDT --side short --outcome stop_loss --entry-quality poor --notes "squeeze after entry"
python3 scripts/public_market_data.py feedback-engine-status
python3 scripts/public_market_data.py feedback-engine-status --symbol ADAUSDT --side short
python3 scripts/public_market_data.py feedback-engine-ingest
python3 scripts/public_market_data.py liquidation-status
python3 scripts/public_market_data.py liquidation-status DOGEUSDT
python3 scripts/public_market_data.py liquidation-capture BTCUSDT ETHUSDT DOGEUSDT --seconds 20 --min-notional 25000
python3 scripts/public_market_data.py state-status
python3 scripts/public_market_data.py state-status XRPUSDT
python3 scripts/public_market_data.py state-sync --top 5 --universe 20 --enrich-top 2 --tv-top 2
python3 scripts/public_market_data.py scan --top 5 --enrich-top 2 --tv-top 2 --persist-state
python3 scripts/public_market_data.py watchlist-refresh --top 5 --max-symbols 12 --enrich-top 2 --tv-top 2 --persist-state
python3 scripts/public_market_data.py pending-alerts --limit 10
python3 scripts/public_market_data.py automation-status
python3 scripts/public_market_data.py automation-install
python3 scripts/public_market_data.py automation-install --enabled --model openai-codex/gpt-5.4-mini
python3 scripts/public_market_data.py alert-mark-sent XRPUSDT --side long
python3 scripts/public_market_data.py tv-status
python3 scripts/public_market_data.py tv-ta HYPEUSDT --timeframe 15m
python3 scripts/public_market_data.py tv-mtf HYPEUSDT
python3 scripts/public_market_data.py tv-volume HYPEUSDT --timeframe 15m
python3 scripts/public_market_data.py tv-confirm HYPEUSDT --bull-score 4
python3 scripts/public_market_data.py snapshot DOGEUSDT
python3 scripts/validate_stack.py
python3 scripts/validate_stack.py --deep-secondary
```

Optional authenticated providers are loaded from workspace `.env`:

- `MASSIVE_API_KEY`
- `COINMARKETCAP_API_KEY`
- `NANSEN_API_KEY`
- `LUNARCRUSH_API_KEY`
- `TRADINGVIEW_MCP_PATH` for a local `tradingview-mcp` checkout or install path

Current local rate-limit guards:

- Massive: 5 requests per minute
- CoinMarketCap: 10,000 requests per month
- Nansen: 20 requests per second and 300 per minute
- LunarCrush: 4 requests per minute and 100 per day

Use `provider-status` to confirm keys are loaded and inspect local usage counters before larger runs.

Provider wiring policy inside the scan pipeline:

- Score the whole universe with exchange-native data first.
- Apply execution filter and stored feedback before finalist enrichment.
- Pull Massive grouped daily once per scan.
- Apply Massive custom bars and LunarCrush only to the top finalists.
- Apply TradingView confirmation only to the top finalists.
- Keep enrichment narrow so rate limits stay safe and scan latency stays reasonable.
- Treat provider entitlement or subscription failures as non-fatal, but report them explicitly in the result.
- Apply the formal grade lock after scoring and again after finalist overlays so alert decisions stay objective.
- Persist the Phase 2 setup lifecycle only through explicit sync paths so normal ad hoc scans stay readable and deterministic.
- Install Phase 3 cron automation in safe no-deliver mode first, validate it, then enable announce delivery only when desired.
- Keep cron model selection explicit when quota pressure or provider limits are unstable; the installer now supports an optional model override.
- Use Phase 4 CVD only as a tape-confirmation layer. It should refine execution quality, not overpower spread, depth, funding, OI, crowding, or invalidation logic.
- Use Phase 5 liquidation intelligence only when recent local `forceOrder` capture exists. It should help judge flushes and squeeze exhaustion, not replace the main scan engine.
- Use Phase 6 TradingView cleanup as a reliability layer, not a new signal layer. The goal is cleaner interval handling, cleaner symbol normalization, and graceful structured errors.
- Use Phase 7 feedback engine v2 as an adaptive modifier, not a replacement for the core scan. It should learn from real outcomes, auto-ingest state-machine drift carefully, and only penalize or support setups when there is enough evidence.

Execution-filter policy in this workspace:

- Penalize wide spreads, thin top-of-book depth, weak tape count, and hot OI expansions.
- Penalize squeeze-prone lower-liquidity shorts more aggressively than clean liquid names.
- Persist real trade feedback in `state/execution_feedback.json` and use it as a scoring modifier.
- Convert runner and no-fill history into management guidance, not only direction changes.

TradingView adapter policy in this workspace:

- Use it as a secondary validator, not as the primary execution engine.
- High-value tools to consume are `coin_analysis`, `multi_timeframe_analysis`, `volume_confirmation_analysis`, and optionally `bollinger_scan`.
- Let TradingView confirm or challenge finalists after Binance, Bybit, event, and Massive checks.
- Do not let TradingView override futures-native funding, OI, spread, or venue-specific risk signals.
- Default permanent local path is `/root/.openclaw/skills/tradingview-mcp/src`, and `TRADINGVIEW_MCP_PATH` can override it when needed.
- Scoring should be asymmetric: small bonuses for agreement, larger penalties for cross-layer conflict, especially when multi-timeframe alignment and technical cluster disagree with the futures bias.

Massive policy in this workspace:

- Use `grouped daily` for broad spot context, breadth rank, and relative participation.
- Use `prev bar` for baseline confirmation.
- Use `custom bars` for finalist-only momentum confirmation, trend efficiency, and close-location quality.
- Do not rely on Massive crypto snapshot endpoints unless entitlement changes.

### references/endpoint-mapping.md
Read this when you need:
- endpoint-to-analysis mapping
- source priority by data layer
- which sources are public now versus later with credentials
- what each endpoint contributes to signal quality
- what the event layer contributes to execution filtering

### references/core-stack-baseline.md
Read this when you need:
- the frozen Phase 0 stack definition
- what counts as core versus secondary versus deferred
- hard-fail versus degraded validation policy
- which validation command to run before later phases

### references/grade-lock.md
Read this when you need:
- the formal `A+ / A / B / C / NO_TRADE` thresholds
- hard disqualifier rules
- alert eligibility versus watchlist eligibility
- why some high-score names still get rejected

### references/state-machine.md
Read this when you need:
- the Phase 2 lifecycle states
- persistent watchlist and alert state files
- setup versioning and alert dedup logic
- which commands mutate state versus only inspect it

### references/hybrid-automation.md
Read this when you need:
- the Phase 3 cron architecture
- discovery versus watchlist refresh responsibilities
- safe no-deliver installation versus announce mode
- validation rules for automation rollout

### references/cvd-from-aggtrades.md
Read this when you need:
- the Phase 4 CVD metrics derived from Binance aggTrades
- tape support versus divergence rules
- how CVD should influence execution quality without overpowering the rest of the stack

### references/liquidation-intelligence.md
Read this when you need:
- the Phase 5 liquidation capture and clustering model
- how nearby `forceOrder` events should support or penalize execution quality
- what counts as flush context versus squeeze-exhaustion context

### references/tradingview-cleanup.md
Read this when you need:
- the Phase 6 TradingView cleanup scope
- how interval normalization is separated for service logic versus `tradingview_ta`
- how warnings and malformed responses should degrade gracefully

### references/feedback-engine-v2.md
Read this when you need:
- the Phase 7 adaptive feedback engine design
- weighted outcome scoring and TP/SL guidance rules
- auto-ingestion behavior from setup state transitions

## Decision rules

- Prefer pullback entries over chasing expansion.
- Prefer no-trade over low-quality execution.
- Prefer Binance plus Bybit alignment over single-venue conviction.
- Treat fresh adverse news or macro event proximity as confidence reducers.
- Treat token unlock headlines, exploit headlines, and exchange operation headlines as execution-quality penalties until disproven.
- Treat liquidation and crowding data as execution filters, not decoration.

## Output expectations

When answering the user after using this skill, provide:
- ranked opportunities or one focused setup
- formal grade when it materially helps
- direction
- entry zone
- stop
- TP1, TP2, TP3
- leverage guidance
- invalidation condition
- why the trade is valid or invalid
- explicit note on missing or unavailable sources
