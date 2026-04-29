"""
openclaw_engine/setup/__init__.py

Actionable setup plan computation — entry zone, stop loss, TP ladder.
Extracted from public_market_data.py.

Import: from openclaw_engine.setup import actionable_setup_plan, round_price, ...
"""
from .plan import (
    price_decimals,
    round_price,
    normalize_take_profit_ladder,
    actionable_setup_plan,
)

__all__ = [
    "price_decimals",
    "round_price",
    "normalize_take_profit_ladder",
    "actionable_setup_plan",
]