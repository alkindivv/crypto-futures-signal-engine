#!/usr/bin/env python3
from __future__ import annotations

import glob
import importlib
import os
import sys
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional


class TradingViewAdapter:
    def __init__(self) -> None:
        self.available = False
        self.import_error: Optional[str] = None
        self.module_root: Optional[str] = None
        self.candidates = self._candidate_paths()
        self._services: Dict[str, Any] = {}
        self._load()

    def _candidate_paths(self) -> List[str]:
        candidates: List[str] = []
        env_candidates = [
            os.environ.get("TRADINGVIEW_MCP_PATH"),
            os.environ.get("TRADINGVIEW_MCP_REPO"),
        ]
        for item in env_candidates:
            if item:
                candidates.append(item)

        home = Path.home()
        candidates.extend([
            str(home / ".openclaw" / "skills" / "tradingview-mcp" / "src"),
            str(home / ".openclaw" / "workspace" / ".openclaw" / "skills" / "tradingview-mcp" / "src"),
            str(home / "tradingview-mcp" / "src"),
            str(Path.cwd() / "vendor" / "tradingview-mcp" / "src"),
        ])
        candidates.extend(glob.glob(os.path.expanduser("~/.local/share/uv/tools/tradingview-mcp-server/lib/python*/site-packages")))
        candidates.extend(glob.glob("/tmp/tradingview-mcp*/src"))

        deduped: List[str] = []
        seen = set()
        for item in candidates:
            if not item:
                continue
            path = str(Path(item).expanduser())
            if path in seen:
                continue
            seen.add(path)
            deduped.append(path)
        return deduped

    def _load(self) -> None:
        last_error = "tradingview_mcp package not found"
        for candidate in self.candidates:
            candidate_path = Path(candidate).expanduser()
            if not candidate_path.exists():
                continue
            inserted = False
            try:
                if str(candidate_path) not in sys.path:
                    sys.path.insert(0, str(candidate_path))
                    inserted = True
                importlib.invalidate_caches()
                screener_service = importlib.import_module("tradingview_mcp.core.services.screener_service")
                scanner_service = importlib.import_module("tradingview_mcp.core.services.scanner_service")
                indicators_service = importlib.import_module("tradingview_mcp.core.services.indicators")
                validators = importlib.import_module("tradingview_mcp.core.utils.validators")
                self._services = {
                    "analyze_coin": screener_service.analyze_coin,
                    "run_multi_timeframe_analysis": screener_service.run_multi_timeframe_analysis,
                    "fetch_bollinger_analysis": screener_service.fetch_bollinger_analysis,
                    "volume_confirmation_analyze": scanner_service.volume_confirmation_analyze,
                    "compute_metrics": getattr(screener_service, "compute_metrics", None),
                    "tv_get_multiple_analysis": getattr(screener_service, "get_multiple_analysis", None),
                    "extract_extended_indicators": indicators_service.extract_extended_indicators,
                    "analyze_timeframe_context": indicators_service.analyze_timeframe_context,
                    "sanitize_exchange": validators.sanitize_exchange,
                    "sanitize_timeframe": validators.sanitize_timeframe,
                }
                self.available = True
                self.module_root = str(candidate_path)
                self.import_error = None
                return
            except Exception as exc:
                last_error = str(exc)
                if inserted:
                    try:
                        sys.path.remove(str(candidate_path))
                    except ValueError:
                        pass
        self.available = False
        self.import_error = last_error

    def status(self) -> Dict[str, Any]:
        return {
            "available": self.available,
            "moduleRoot": self.module_root,
            "importError": self.import_error,
            "candidates": self.candidates,
            "highValueTools": [
                "coin_analysis",
                "multi_timeframe_analysis",
                "volume_confirmation_analysis",
                "bollinger_scan",
            ],
        }

    def _require(self) -> None:
        if not self.available:
            raise RuntimeError(self.import_error or "tradingview_mcp is unavailable")

    def _component_available(self, payload: Any) -> bool:
        return isinstance(payload, dict) and not payload.get("error")

    def _bias_direction(self, bull_score: int) -> str:
        if bull_score >= 2:
            return "bullish"
        if bull_score <= -2:
            return "bearish"
        return "neutral"

    def _clamp(self, value: float, lower: float, upper: float) -> float:
        return max(lower, min(upper, value))

    def _normalize_error(self, err: Any) -> str:
        message = str(err or "unknown error").strip()
        if "Expecting value: line 1 column 1" in message:
            return "TradingView returned an empty or malformed response"
        if "No data found" in message:
            return message
        if "Interval is empty or not valid" in message:
            return "TradingView rejected the requested interval"
        return message

    def _call_with_warnings(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = func(*args, **kwargs)
        warning_messages = [str(item.message) for item in caught if str(item.message).strip()]
        return result, warning_messages

    def _normalize_symbol(self, symbol: str, exchange: str) -> str:
        cleaned = str(symbol or "").upper().strip()
        if ":" in cleaned:
            return cleaned
        return f"{exchange.upper()}:{cleaned}"

    def _service_timeframe(self, timeframe: str = "15m") -> str:
        normalized = str(timeframe or "15m").strip().lower()
        aliases = {
            "5": "5m",
            "5m": "5m",
            "15": "15m",
            "15m": "15m",
            "1h": "1h",
            "60": "1h",
            "60m": "1h",
            "4h": "4h",
            "240": "4h",
            "240m": "4h",
            "1d": "1D",
            "d": "1D",
            "day": "1D",
            "1w": "1W",
            "w": "1W",
            "week": "1W",
            "1mo": "1M",
            "1mon": "1M",
            "1month": "1M",
            "month": "1M",
            "1mth": "1M",
        }
        return aliases.get(normalized, "15m")

    def _ta_timeframe(self, timeframe: str = "15m") -> str:
        normalized = str(timeframe or "15m").strip().lower()
        aliases = {
            "5": "5m",
            "5m": "5m",
            "15": "15m",
            "15m": "15m",
            "1h": "1h",
            "60": "1h",
            "60m": "1h",
            "4h": "4h",
            "240": "4h",
            "240m": "4h",
            "1d": "1d",
            "d": "1d",
            "day": "1d",
            "1w": "1W",
            "w": "1W",
            "week": "1W",
            "1mo": "1M",
            "1mon": "1M",
            "1month": "1M",
            "month": "1M",
            "1mth": "1M",
        }
        return aliases.get(normalized, "15m")

    def _payload_missing(self, payload: Any) -> bool:
        if payload is None:
            return True
        if isinstance(payload, dict) and payload.get("error"):
            return True
        return False

    def sanitize_exchange(self, exchange: str = "binance") -> str:
        self._require()
        return self._services["sanitize_exchange"](exchange, "binance")

    def sanitize_timeframe(self, timeframe: str = "15m") -> str:
        self._require()
        return self._service_timeframe(timeframe)

    def coin_analysis(self, symbol: str, exchange: str = "binance", timeframe: str = "15m") -> Dict[str, Any]:
        self._require()
        exchange = self.sanitize_exchange(exchange)
        timeframe = self._ta_timeframe(timeframe)
        try:
            payload, warning_messages = self._call_with_warnings(self._services["analyze_coin"], symbol.upper(), exchange, timeframe)
            if not isinstance(payload, dict):
                return {"error": "TradingView technical analysis returned an invalid payload"}
            if warning_messages:
                payload["warnings"] = warning_messages
            return payload
        except Exception as exc:
            return {"error": self._normalize_error(exc)}

    def multi_timeframe_analysis(self, symbol: str, exchange: str = "binance") -> Dict[str, Any]:
        self._require()
        exchange = self.sanitize_exchange(exchange)
        full_symbol = self._normalize_symbol(symbol, exchange)
        get_multiple_analysis = self._services.get("tv_get_multiple_analysis")
        compute_metrics = self._services.get("compute_metrics")
        extract_extended = self._services.get("extract_extended_indicators")
        analyze_context = self._services.get("analyze_timeframe_context")
        if not get_multiple_analysis or not compute_metrics or not extract_extended or not analyze_context:
            return {"error": "TradingView multi-timeframe dependencies are incomplete"}

        screener = "crypto"
        timeframes = [
            ("1W", "1W"),
            ("1D", "1d"),
            ("4h", "4h"),
            ("1h", "1h"),
            ("15m", "15m"),
        ]
        tf_labels = {
            "1W": "Weekly (Trend Bias)",
            "1D": "Daily (Swing Setup)",
            "4h": "4-Hour (Refinement)",
            "1h": "1-Hour (Entry Timing)",
            "15m": "15-Min (Execution)",
        }
        tf_results: Dict[str, Any] = {}
        alignment_scores: List[int] = []
        warnings_seen: List[str] = []

        for label, ta_interval in timeframes:
            try:
                analysis, warning_messages = self._call_with_warnings(get_multiple_analysis, screener=screener, interval=ta_interval, symbols=[full_symbol])
                warnings_seen.extend(warning_messages)
                if not isinstance(analysis, dict) or full_symbol not in analysis or analysis.get(full_symbol) is None:
                    tf_results[label] = {"error": f"No data for {label}"}
                    continue
                data = analysis[full_symbol]
                indicators = getattr(data, "indicators", None)
                if not isinstance(indicators, dict) or not indicators:
                    tf_results[label] = {"error": f"No indicator data for {label}"}
                    continue
                metrics = compute_metrics(indicators)
                extended = extract_extended(indicators)
                tf_context = analyze_context(indicators, label)
                bias_num = 1 if tf_context.get("bias") == "Bullish" else -1 if tf_context.get("bias") == "Bearish" else 0
                alignment_scores.append(bias_num)
                tf_results[label] = {
                    "label": tf_labels.get(label, label),
                    "bias": tf_context.get("bias"),
                    "bias_reasons": tf_context.get("bias_reasons", []),
                    "key_indicators": tf_context.get("key_indicators_for_timeframe", []),
                    "advice": tf_context.get("advice"),
                    "price": metrics.get("price") if isinstance(metrics, dict) else None,
                    "change_pct": metrics.get("change") if isinstance(metrics, dict) else None,
                    "rsi": ((extended.get("rsi") or {}).get("value") if isinstance(extended, dict) else None),
                    "macd_crossover": (((extended.get("macd") or {}).get("crossover")) if isinstance(extended, dict) else None),
                    "ema_trend": {
                        "ema20": ((extended.get("ema") or {}).get("ema20") if isinstance(extended, dict) else None),
                        "ema50": ((extended.get("ema") or {}).get("ema50") if isinstance(extended, dict) else None),
                        "ema200": ((extended.get("ema") or {}).get("ema200") if isinstance(extended, dict) else None),
                    },
                    "volume_signal": (((extended.get("volume") or {}).get("signal")) if isinstance(extended, dict) else None),
                    "market_structure": (((extended.get("market_structure") or {}).get("trend")) if isinstance(extended, dict) else None),
                    "trend_strength": (((extended.get("market_structure") or {}).get("trend_strength")) if isinstance(extended, dict) else None),
                    "momentum_aligned": (((extended.get("market_structure") or {}).get("momentum_aligned")) if isinstance(extended, dict) else None),
                    "warnings": warning_messages,
                }
            except Exception as exc:
                tf_results[label] = {"error": self._normalize_error(exc)}

        total_score = sum(alignment_scores)
        all_bullish = all(score > 0 for score in alignment_scores) if alignment_scores else False
        all_bearish = all(score < 0 for score in alignment_scores) if alignment_scores else False
        if all_bullish:
            alignment, confidence, action = "FULLY ALIGNED BULLISH", "Very High", "STRONG BUY - All timeframes bullish. Look for pullback entry on 1H/15m."
        elif all_bearish:
            alignment, confidence, action = "FULLY ALIGNED BEARISH", "Very High", "STRONG SELL - All timeframes bearish. Avoid longs."
        elif total_score >= 3:
            alignment, confidence, action = "MOSTLY BULLISH", "High", "BUY - Majority of timeframes bullish. Enter on 4H/1H pullback to support."
        elif total_score <= -3:
            alignment, confidence, action = "MOSTLY BEARISH", "High", "SELL - Majority of timeframes bearish. Avoid catching the falling knife."
        elif total_score > 0:
            alignment, confidence, action = "LEAN BULLISH", "Medium", "CAUTIOUS BUY - Some bullish signals but not fully aligned. Wait for better setup."
        elif total_score < 0:
            alignment, confidence, action = "LEAN BEARISH", "Medium", "CAUTIOUS SELL - Some bearish signals. Reduce position or wait."
        else:
            alignment, confidence, action = "MIXED/RANGING", "Low", "HOLD/NO TRADE - Timeframes conflict. Wait for alignment."

        labels = [label for label, _ in timeframes]
        higher_tf_bias = alignment_scores[0] if alignment_scores else 0
        divergent_tfs = [labels[idx] for idx, score in enumerate(alignment_scores) if score != 0 and score != higher_tf_bias and higher_tf_bias != 0]
        return {
            "symbol": full_symbol,
            "exchange": exchange,
            "analysis_type": "Multi-Timeframe Alignment",
            "timeframes": tf_results,
            "alignment": {
                "status": alignment,
                "confidence": confidence,
                "net_score": total_score,
                "scores_by_tf": dict(zip(labels, alignment_scores)),
                "divergent_timeframes": divergent_tfs,
            },
            "recommendation": {
                "action": action,
                "entry_timeframe": "1H or 4H pullback" if total_score > 0 else "Wait for alignment",
                "rules": [
                    "Weekly sets BIAS (direction only, not entries)",
                    "Daily finds SETUP (swing level, confluence)",
                    "4H refines entry zone",
                    "1H/15m triggers entry with tight stop",
                    "Never trade against Weekly + Daily combined direction",
                ],
            },
            "warnings": warnings_seen,
        }

    def volume_confirmation_analysis(self, symbol: str, exchange: str = "binance", timeframe: str = "15m") -> Dict[str, Any]:
        self._require()
        exchange = self.sanitize_exchange(exchange)
        timeframe = self._ta_timeframe(timeframe)
        full_symbol = self._normalize_symbol(symbol, exchange)
        try:
            payload, warning_messages = self._call_with_warnings(self._services["volume_confirmation_analyze"], full_symbol, exchange, timeframe)
            if not isinstance(payload, dict):
                return {"error": "TradingView volume confirmation returned an invalid payload"}
            if warning_messages:
                payload["warnings"] = warning_messages
            if payload.get("error"):
                payload["error"] = self._normalize_error(payload.get("error"))
            return payload
        except Exception as exc:
            return {"error": self._normalize_error(exc)}

    def bollinger_scan(self, exchange: str = "binance", timeframe: str = "4h", bbw_threshold: float = 0.04, limit: int = 20) -> Any:
        self._require()
        exchange = self.sanitize_exchange(exchange)
        timeframe = self._ta_timeframe(timeframe)
        try:
            payload, warning_messages = self._call_with_warnings(self._services["fetch_bollinger_analysis"], exchange, timeframe=timeframe, bbw_filter=bbw_threshold, limit=limit)
            if isinstance(payload, list):
                return {"exchange": exchange, "timeframe": timeframe, "warnings": warning_messages, "rows": payload}
            return payload
        except Exception as exc:
            return {"error": self._normalize_error(exc)}

    def finalist_confirmation(self, symbol: str, exchange: str = "binance", timeframe: str = "15m") -> Dict[str, Any]:
        self._require()
        tech = self.coin_analysis(symbol, exchange=exchange, timeframe=timeframe)
        mtf = self.multi_timeframe_analysis(symbol, exchange=exchange)
        volume = self.volume_confirmation_analysis(symbol, exchange=exchange, timeframe=timeframe)
        return {
            "symbol": symbol.upper(),
            "exchange": self.sanitize_exchange(exchange),
            "timeframe": self.sanitize_timeframe(timeframe),
            "technical": tech,
            "multiTimeframe": mtf,
            "volumeConfirmation": volume,
        }

    def overlay(self, symbol: str, bull_score: int, exchange: str = "binance", timeframe: str = "15m") -> Dict[str, Any]:
        confirmation = self.finalist_confirmation(symbol, exchange=exchange, timeframe=timeframe)
        clean_delta = 0.0
        notes: List[str] = []
        support_count = 0
        conflict_count = 0

        tech = confirmation.get("technical", {}) if isinstance(confirmation, dict) else {}
        mtf = confirmation.get("multiTimeframe", {}) if isinstance(confirmation, dict) else {}
        volume = confirmation.get("volumeConfirmation", {}) if isinstance(confirmation, dict) else {}

        tech_ok = self._component_available(tech)
        mtf_ok = self._component_available(mtf)
        volume_ok = self._component_available(volume)
        available_components = [name for name, ok in (("technical", tech_ok), ("multiTimeframe", mtf_ok), ("volumeConfirmation", volume_ok)) if ok]
        data_quality = round(len(available_components) / 3, 2)
        bias = self._bias_direction(bull_score)
        strong_bias = abs(bull_score) >= 4

        if isinstance(tech, dict) and tech.get("error"):
            notes.append(f"TradingView technical analysis unavailable: {tech.get('error')}")
        if isinstance(mtf, dict) and mtf.get("error"):
            notes.append(f"TradingView multi-timeframe analysis unavailable: {mtf.get('error')}")
        if isinstance(volume, dict) and volume.get("error"):
            notes.append(f"TradingView volume confirmation unavailable: {volume.get('error')}")

        if bias == "neutral":
            return {
                "symbol": symbol.upper(),
                "bias": bias,
                "dataQuality": data_quality,
                "availableComponents": available_components,
                "supportCount": support_count,
                "conflictCount": conflict_count,
                "cleanScoreDelta": 0.0,
                "notes": notes,
                "confirmation": confirmation,
            }

        mtf_alignment = (mtf.get("alignment") or {}) if isinstance(mtf, dict) else {}
        mtf_score = mtf_alignment.get("net_score")
        mtf_status = str(mtf_alignment.get("status") or "").upper()
        mtf_confidence = str(mtf_alignment.get("confidence") or "").upper()
        try:
            if mtf_score is not None:
                mtf_score = int(mtf_score)
                if bias == "bullish":
                    if mtf_score >= 4:
                        clean_delta += 1.1 if mtf_confidence == "HIGH" else 0.9
                        support_count += 1
                        notes.append("TradingView multi-timeframe alignment strongly supports the bullish setup")
                    elif mtf_score >= 2:
                        clean_delta += 0.6
                        support_count += 1
                        notes.append("TradingView multi-timeframe alignment supports the bullish setup")
                    elif mtf_score <= -4:
                        clean_delta -= 2.6 if strong_bias else 2.1
                        conflict_count += 1
                        notes.append("TradingView multi-timeframe alignment strongly conflicts with the bullish setup")
                    elif mtf_score < 0:
                        clean_delta -= 1.7 if strong_bias else 1.35
                        conflict_count += 1
                        notes.append("TradingView multi-timeframe alignment conflicts with the bullish setup")
                    elif "MIXED" in mtf_status or "RANGING" in mtf_status:
                        clean_delta -= 0.9 if strong_bias else 0.55
                        conflict_count += 1
                        notes.append("TradingView multi-timeframe view is mixed for a bullish futures setup")
                elif bias == "bearish":
                    if mtf_score <= -4:
                        clean_delta += 1.1 if mtf_confidence == "HIGH" else 0.9
                        support_count += 1
                        notes.append("TradingView multi-timeframe alignment strongly supports the bearish setup")
                    elif mtf_score <= -2:
                        clean_delta += 0.6
                        support_count += 1
                        notes.append("TradingView multi-timeframe alignment supports the bearish setup")
                    elif mtf_score >= 4:
                        clean_delta -= 2.6 if strong_bias else 2.1
                        conflict_count += 1
                        notes.append("TradingView multi-timeframe alignment strongly conflicts with the bearish setup")
                    elif mtf_score > 0:
                        clean_delta -= 1.7 if strong_bias else 1.35
                        conflict_count += 1
                        notes.append("TradingView multi-timeframe alignment conflicts with the bearish setup")
                    elif "MIXED" in mtf_status or "RANGING" in mtf_status:
                        clean_delta -= 0.9 if strong_bias else 0.55
                        conflict_count += 1
                        notes.append("TradingView multi-timeframe view is mixed for a bearish futures setup")
        except Exception:
            pass

        market_sentiment = (tech.get("market_sentiment") or {}) if isinstance(tech, dict) else {}
        buy_sell_signal = str(market_sentiment.get("buy_sell_signal") or "").upper()
        momentum = str(market_sentiment.get("momentum") or "").upper()
        adx = (tech.get("adx") or {}) if isinstance(tech, dict) else {}
        adx_strength = str(adx.get("trend_strength") or "")
        technical_volume = (tech.get("volume_analysis") or {}) if isinstance(tech, dict) else {}
        technical_volume_signal = str(technical_volume.get("signal") or "")
        try:
            if bias == "bullish":
                if buy_sell_signal == "BUY":
                    clean_delta += 0.8
                    support_count += 1
                    notes.append("TradingView technical cluster prints BUY")
                elif buy_sell_signal == "SELL":
                    clean_delta -= 1.35 if strong_bias else 1.0
                    conflict_count += 1
                    notes.append("TradingView technical cluster leans SELL against the bullish setup")
                if momentum == "BULLISH":
                    clean_delta += 0.25
                elif momentum == "BEARISH":
                    clean_delta -= 0.55
                    conflict_count += 1
                    notes.append("TradingView momentum reads bearish against the bullish setup")
            elif bias == "bearish":
                if buy_sell_signal == "SELL":
                    clean_delta += 0.8
                    support_count += 1
                    notes.append("TradingView technical cluster prints SELL")
                elif buy_sell_signal == "BUY":
                    clean_delta -= 1.35 if strong_bias else 1.0
                    conflict_count += 1
                    notes.append("TradingView technical cluster leans BUY against the bearish setup")
                if momentum == "BEARISH":
                    clean_delta += 0.25
                elif momentum == "BULLISH":
                    clean_delta -= 0.55
                    conflict_count += 1
                    notes.append("TradingView momentum reads bullish against the bearish setup")
            if "Strong Trend" in adx_strength:
                clean_delta += 0.35
                notes.append("TradingView ADX shows a strong trend regime")
            if technical_volume_signal in {"Very High", "High", "Above Average"}:
                clean_delta += 0.2
                notes.append("TradingView technical volume profile is supportive")
        except Exception:
            pass

        overall_assessment = (volume.get("overall_assessment") or {}) if isinstance(volume, dict) else {}
        bullish_signals = int(overall_assessment.get("bullish_signals", 0) or 0)
        bearish_signals = int(overall_assessment.get("bearish_signals", 0) or 0)
        warning_signals = int(overall_assessment.get("warning_signals", 0) or 0)
        try:
            if bias == "bullish":
                if bullish_signals > bearish_signals and warning_signals == 0:
                    clean_delta += 0.65
                    support_count += 1
                    notes.append("TradingView volume confirmation favors bullish continuation")
                elif bearish_signals > bullish_signals:
                    clean_delta -= 1.05 if warning_signals == 0 else 1.35
                    conflict_count += 1
                    notes.append("TradingView volume confirmation warns against the bullish setup")
            elif bias == "bearish":
                if bearish_signals > bullish_signals and warning_signals == 0:
                    clean_delta += 0.65
                    support_count += 1
                    notes.append("TradingView volume confirmation favors bearish continuation")
                elif bullish_signals > bearish_signals:
                    clean_delta -= 1.05 if warning_signals == 0 else 1.35
                    conflict_count += 1
                    notes.append("TradingView volume confirmation warns against the bearish setup")
            if warning_signals > 0:
                clean_delta -= min(0.3 * warning_signals, 1.0)
                notes.append("TradingView volume layer reports cautionary signals")
        except Exception:
            pass

        if conflict_count >= 2:
            clean_delta -= 0.9 if strong_bias else 0.65
            notes.append("TradingView conflict stack is large enough to downgrade the setup")
        elif support_count >= 2 and conflict_count == 0:
            clean_delta += 0.4
            notes.append("TradingView confirmation stack is clean across multiple layers")

        if data_quality < 0.67 and clean_delta > 0:
            clean_delta *= 0.6
            notes.append("TradingView positive impact is capped because confirmation coverage is incomplete")
        elif data_quality < 0.34 and clean_delta < 0:
            clean_delta *= 0.75
            notes.append("TradingView penalty is softened because confirmation coverage is very thin")

        clean_delta = self._clamp(round(clean_delta, 2), -4.5, 2.5)
        return {
            "symbol": symbol.upper(),
            "bias": bias,
            "dataQuality": data_quality,
            "availableComponents": available_components,
            "supportCount": support_count,
            "conflictCount": conflict_count,
            "cleanScoreDelta": clean_delta,
            "notes": notes,
            "confirmation": confirmation,
        }
