
from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Optional
from urllib.parse import urlencode

import requests


@dataclass(frozen=True)
class SaxoInstrument:
    symbol: str
    uic: int
    asset_type: str = "Stock"
    currency: Optional[str] = None
    tick_size: Optional[float] = None
    order_decimals: Optional[int] = None


@dataclass(frozen=True)
class SaxoTradeResult:
    status: str
    symbol: str
    message: str
    payload: Optional[dict[str, Any]] = None


class SaxoApiError(RuntimeError):
    pass


class SaxoTrader:
    def __init__(self) -> None:
        self.enabled = os.getenv("SAXO_TRADING_ENABLED", "false").lower() == "true"
        self.env = os.getenv("SAXO_ENV", "sim").lower()
        self.base_url = (
            "https://gateway.saxobank.com/openapi"
            if self.env == "live"
            else "https://gateway.saxobank.com/sim/openapi"
        )
        self.account_key = os.getenv("SAXO_ACCOUNT_KEY", "")
        self.budget_dkk = float(os.getenv("SAXO_ORDER_BUDGET_DKK", "500"))
        self.tp_pct = float(os.getenv("SAXO_TAKE_PROFIT_PCT", "8")) / 100.0
        self.trailing_stop_pct = float(os.getenv("SAXO_TRAILING_STOP_PCT", "4")) / 100.0
        self.trailing_step_pct = float(os.getenv("SAXO_TRAILING_STEP_PCT", "1")) / 100.0
        self.max_open_positions = int(os.getenv("SAXO_MAX_OPEN_POSITIONS", "5"))
        self.dry_run = os.getenv("SAXO_DRY_RUN", "true").lower() == "true"
        self.access_token = os.getenv("SAXO_ACCESS_TOKEN", "")
        self.symbol_map = self._load_symbol_map()

    def _load_symbol_map(self) -> dict[str, dict[str, Any]]:
        raw = os.getenv("SAXO_SYMBOL_MAP_JSON", "{}")
        try:
            data = json.loads(raw)
            return {k.upper(): v for k, v in data.items()}
        except Exception as exc:
            raise SaxoApiError(f"Invalid SAXO_SYMBOL_MAP_JSON: {exc}") from exc

    def _headers(self) -> dict[str, str]:
        if not self.access_token:
            raise SaxoApiError("Missing SAXO_ACCESS_TOKEN. Use a SIM token first, then implement OAuth refresh for live.")
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, *, params: Optional[dict[str, Any]] = None,
                 json_body: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        response = requests.request(
            method,
            url,
            headers=self._headers(),
            params=params,
            json=json_body,
            timeout=20,
        )
        try:
            data = response.json() if response.text else {}
        except Exception:
            data = {"raw": response.text}
        if response.status_code >= 400:
            raise SaxoApiError(f"Saxo API {response.status_code} {method} {url}: {data}")
        return data

    def resolve_instrument(self, symbol: str) -> SaxoInstrument:
        sym = symbol.upper().strip()
        mapped = self.symbol_map.get(sym)
        if mapped:
            return SaxoInstrument(
                symbol=sym,
                uic=int(mapped["Uic"]),
                asset_type=mapped.get("AssetType", "Stock"),
                currency=mapped.get("CurrencyCode"),
                tick_size=mapped.get("TickSize"),
                order_decimals=mapped.get("OrderDecimals"),
            )

   
        params = {
            "Keywords": sym,
            "AssetTypes": "Stock",
            "IncludeNonTradable": "false",
        }
        result = self._request("GET", "/ref/v1/instruments", params=params)
        candidates = result.get("Data", []) if isinstance(result, dict) else []
        exact = [x for x in candidates if str(x.get("Symbol", "")).split(":")[0].upper() == sym]
        chosen = exact[0] if exact else (candidates[0] if candidates else None)
        if not chosen:
            raise SaxoApiError(f"Could not resolve Saxo instrument for {sym}. Add it to SAXO_SYMBOL_MAP_JSON.")

        uic = int(chosen.get("Identifier") or chosen.get("Uic"))
        asset_type = chosen.get("AssetType", "Stock")
        details = self._request(
            "GET",
            f"/ref/v1/instruments/details/{uic}/{asset_type}",
            params={"AccountKey": self.account_key} if self.account_key else None,
        )
        fmt = details.get("Format", {}) or {}
        return SaxoInstrument(
            symbol=sym,
            uic=uic,
            asset_type=asset_type,
            currency=details.get("CurrencyCode"),
            tick_size=details.get("TickSize"),
            order_decimals=fmt.get("OrderDecimals"),
        )

    def info_price(self, inst: SaxoInstrument, amount: int = 1) -> dict[str, Any]:
        params = {
            "Uic": inst.uic,
            "AssetType": inst.asset_type,
            "Amount": amount,
            "FieldGroups": "Quote,DisplayAndFormat,PriceInfo",
        }
        if self.account_key:
            params["AccountKey"] = self.account_key
        return self._request("GET", "/trade/v1/infoprices", params=params)

    @staticmethod
    def _extract_ask(price_response: dict[str, Any]) -> float:
        quote = price_response.get("Quote", {}) or {}
        ask = quote.get("Ask") or quote.get("Mid") or quote.get("Price")
        if ask is None:
            raise SaxoApiError(f"Could not find tradable Ask/Mid in price response: {price_response}")
        return float(ask)

    @staticmethod
    def _round_price(value: float, tick_size: Optional[float], decimals: Optional[int]) -> float:
        if tick_size and tick_size > 0:
            q = Decimal(str(tick_size))
            return float((Decimal(str(value)) / q).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * q)
        if decimals is not None:
            return round(value, int(decimals))
        return round(value, 2)

    def _build_market_buy_with_related_orders(self, inst: SaxoInstrument, qty: int, ask: float) -> dict[str, Any]:
        take_profit = self._round_price(ask * (1 + self.tp_pct), inst.tick_size, inst.order_decimals)
        stop_price = self._round_price(ask * (1 - self.trailing_stop_pct), inst.tick_size, inst.order_decimals)
        trailing_distance = self._round_price(ask * self.trailing_stop_pct, inst.tick_size, inst.order_decimals)
        trailing_step = self._round_price(ask * self.trailing_step_pct, inst.tick_size, inst.order_decimals)

        order: dict[str, Any] = {
            "Uic": inst.uic,
            "AssetType": inst.asset_type,
            "BuySell": "Buy",
            "Amount": qty,
            "OrderType": "Market",
            "ManualOrder": False,
            "OrderDuration": {"DurationType": "DayOrder"},
            "Orders": [
                {
                    "BuySell": "Sell",
                    "Amount": qty,
                    "OrderType": "Limit",
                    "OrderPrice": take_profit,
                    "ManualOrder": False,
                    "OrderDuration": {"DurationType": "GoodTillCancel"},
                },
                {
                    "BuySell": "Sell",
                    "Amount": qty,
                    "OrderType": "TrailingStopIfTraded",
                    "OrderPrice": stop_price,
                    "TrailingStopDistanceToMarket": trailing_distance,
                    "TrailingStopStep": trailing_step,
                    "ManualOrder": False,
                    "OrderDuration": {"DurationType": "GoodTillCancel"},
                },
            ],
        }
        if self.account_key:
            order["AccountKey"] = self.account_key
        return order

    def precheck_order(self, order: dict[str, Any]) -> dict[str, Any]:
        body = dict(order)
        body["FieldGroups"] = ["Costs"]
        return self._request("POST", "/trade/v2/orders/precheck", json_body=body)

    def place_order(self, order: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/trade/v2/orders", json_body=order)

    def handle_alert(self, symbol: str) -> SaxoTradeResult:
        sym = symbol.upper().strip()
        if not self.enabled:
            return SaxoTradeResult("DISABLED", sym, "SAXO_TRADING_ENABLED is false; alert only.")

        inst = self.resolve_instrument(sym)
        price_response = self.info_price(inst, amount=1)
        ask = self._extract_ask(price_response)

        qty = math.floor(self.budget_dkk / ask)
        if qty < 1:
            return SaxoTradeResult(
                "SKIPPED_SINGLE_SHARE_TOO_EXPENSIVE",
                sym,
                f"Ask {ask:.4f} is above budget {self.budget_dkk:.2f}; alert only.",
                {"ask": ask, "budget": self.budget_dkk},
            )

        order = self._build_market_buy_with_related_orders(inst, qty, ask)
        precheck = self.precheck_order(order)

        if self.dry_run:
            return SaxoTradeResult("DRY_RUN_PRECHECK_OK", sym, "Precheck completed; order not sent.", {"order": order, "precheck": precheck})

        placed = self.place_order(order)
        return SaxoTradeResult("ORDER_SENT", sym, f"Submitted market buy for {qty} share(s) with related TP/trailing stop.", placed)
