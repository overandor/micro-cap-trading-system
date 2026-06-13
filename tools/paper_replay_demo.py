#!/usr/bin/env python3
"""
Offline walk-forward demo for gate_micro_mm.py dry-run pipeline.

NO network. Feeds a synthetic micro order book + taker trade prints through
the REAL QuoteEngine + PaperBroker + RiskLedger so you can see the exact log
output a live `MM_DRY_RUN=1` session produces — without an exchange or keys.

Run:  python tools/paper_replay_demo.py
"""
import asyncio
from decimal import Decimal

import gate_micro_mm as mm


async def main() -> None:
    # Model the user's scenario: a -36 USDT drawdown we want to "watch" recover.
    cfg = mm.Config(api_key="", api_secret="")
    cfg.dry_run   = True
    cfg.start_pnl = Decimal("-36")
    cfg.momentum_window = 4   # shorten so the demo isn't suppressed early

    sym  = "PEPE_USDT"
    inst = mm.Instrument(symbol=sym, tick=Decimal("0.0000001"),
                         lot=1, mark=Decimal("0.0000100"))

    rest = mm.GateRest(cfg, session=None)   # dry → never touches the network
    oms  = mm.OMS()
    risk = mm.RiskLedger(cfg)
    book = mm.OrderBook(sym)

    engine = mm.QuoteEngine(sym, inst, cfg, rest, oms, risk, book)
    paper  = mm.PaperBroker(cfg, oms, risk)
    paper.on_fill(engine.on_fill)

    # Synthetic best bid/ask (2-tick spread) that drifts up over time.
    bid = Decimal("0.0000100")
    ask = Decimal("0.0000102")

    mm.log.info("── WALK-FORWARD REPLAY START (synthetic data) ──")
    for step in range(8):
        # publish a book snapshot
        book.apply_snapshot({
            "b": [[str(bid), "9000000"], [str(bid - inst.tick), "5000000"]],
            "a": [[str(ask), "9000000"], [str(ask + inst.tick), "5000000"]],
        })
        await engine.refresh()  # places/repositions paper bid + ask

        # Taker SELL hits our resting BID → our BUY fills
        await paper.on_trade(sym, bid, "sell")
        await engine.refresh()  # re-quote after the fill

        # Price ticks up; taker BUY lifts our resting ASK → our SELL fills (round trip)
        bid += inst.tick
        ask += inst.tick
        book.apply_snapshot({
            "b": [[str(bid), "9000000"]],
            "a": [[str(ask), "9000000"]],
        })
        await engine.refresh()
        await paper.on_trade(sym, ask, "buy")
        await asyncio.sleep(0)

    mm.log.info("── REPLAY END ──  simulated fills: %d", paper.fills)
    mm.log.info(await risk.report())


if __name__ == "__main__":
    asyncio.run(main())
