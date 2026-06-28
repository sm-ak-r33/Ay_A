
from saxo_trader import SaxoApiError, SaxoTrader


self.trader = SaxoTrader()


try:
    trade_result = self.trader.handle_alert(candle.symbol)
    log.info("Saxo result for %s: %s - %s", candle.symbol, trade_result.status, trade_result.message)
    if trade_result.status in {"ORDER_SENT", "DRY_RUN_PRECHECK_OK", "SKIPPED_SINGLE_SHARE_TOO_EXPENSIVE", "DISABLED"}:
     
        pass
except SaxoApiError as exc:
    log.error("Saxo trading failed for %s: %s", candle.symbol, exc)
except Exception as exc:
    log.exception("Unexpected Saxo trading error for %s: %s", candle.symbol, exc)
