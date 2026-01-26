from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class OpportunityConfig:
    min_ev: float = 0.01
    min_ask_cents: float = 1.0
    max_ask_cents: float = 99.0
    max_spot_age: int | None = None
    max_quote_age: int | None = None
    top_n: int | None = None
    emit_passes: bool = False
    best_side_only: bool = True
    model_version: int = 1


def _ask_tradable(
    ask: float | None, min_ask: float, max_ask: float
) -> bool:
    if ask is None:
        return False
    return min_ask <= ask <= max_ask


def _ev_take_yes(prob_yes: float, yes_ask: float) -> float:
    return prob_yes - (yes_ask / 100.0)


def _ev_take_no(prob_yes: float, no_ask: float) -> float:
    return (1.0 - prob_yes) - (no_ask / 100.0)


def _spread(
    bid: float | None, ask: float | None
) -> float | None:
    if bid is None or ask is None:
        return None
    return ask - bid


def build_opportunities_from_snapshots(
    snapshots: list[dict[str, Any]],
    config: OpportunityConfig,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows: list[dict[str, Any]] = []
    counters = {
        "snapshots_total": len(snapshots),
        "takes": 0,
        "passes": 0,
        "skipped": 0,
        "missing_prob": 0,
        "spot_stale": 0,
        "quote_stale": 0,
        "missing_yes_ask": 0,
        "missing_no_ask": 0,
        "ev_below_threshold": 0,
        "top_n_cutoff": 0,
    }

    take_rows: list[dict[str, Any]] = []
    pass_rows: list[dict[str, Any]] = []

    for snap in snapshots:
        asof_ts = snap.get("asof_ts")
        market_id = snap.get("market_id")
        if not market_id or asof_ts is None:
            counters["skipped"] += 1
            continue

        prob_yes = snap.get("prob_yes")
        yes_ask = snap.get("yes_ask")
        no_ask = snap.get("no_ask")
        spot_age = snap.get("spot_age_seconds")
        quote_age = snap.get("quote_age_seconds")

        global_reasons: list[str] = []
        if prob_yes is None:
            global_reasons.append("missing_prob")
            counters["missing_prob"] += 1
        if (
            config.max_spot_age is not None
            and spot_age is not None
            and spot_age > config.max_spot_age
        ):
            global_reasons.append("spot_stale")
            counters["spot_stale"] += 1
        if (
            config.max_quote_age is not None
            and quote_age is not None
            and quote_age > config.max_quote_age
        ):
            global_reasons.append("quote_stale")
            counters["quote_stale"] += 1

        def build_row(
            *,
            side: str,
            ev: float | None,
            ask: float | None,
            reason: str | None,
        ) -> dict[str, Any]:
            strike = snap.get("strike")
            settlement_ts = snap.get("settlement_ts")
            tau_minutes = None
            if snap.get("horizon_seconds") is not None:
                tau_minutes = snap.get("horizon_seconds") / 60.0
            p_market = None
            if snap.get("yes_mid") is not None:
                p_market = snap.get("yes_mid") / 100.0
            elif snap.get("no_mid") is not None:
                p_market = 1.0 - (snap.get("no_mid") / 100.0)
            spread = None
            if side == "YES":
                spread = _spread(snap.get("yes_bid"), snap.get("yes_ask"))
            elif side == "NO":
                spread = _spread(snap.get("no_bid"), snap.get("no_ask"))

            eligible = 1 if reason is None else 0
            would_trade = 1 if ev is not None and ev >= config.min_ev else 0
            decision = "TAKE" if would_trade == 1 else "PASS"
            if reason is None and would_trade == 0:
                reason = "ev_below_threshold"
                counters["ev_below_threshold"] += 1

            metadata = {
                "asof_ts": asof_ts,
                "spot_ts": snap.get("spot_ts"),
                "quote_ts": snap.get("quote_ts"),
                "spot_age_seconds": spot_age,
                "quote_age_seconds": quote_age,
                "price_used_cents": ask,
                "prob_yes": prob_yes,
                "prob_yes_raw": snap.get("prob_yes_raw"),
                "decision": decision,
                "decision_reason": reason,
                "model_version": config.model_version,
            }
            return {
                "ts_eval": asof_ts,
                "market_id": market_id,
                "settlement_ts": settlement_ts,
                "strike": strike,
                "spot_price": snap.get("spot_price"),
                "sigma": snap.get("sigma_annualized"),
                "tau": tau_minutes,
                "p_model": prob_yes,
                "p_market": p_market,
                "best_yes_bid": snap.get("yes_bid"),
                "best_yes_ask": snap.get("yes_ask"),
                "best_no_bid": snap.get("no_bid"),
                "best_no_ask": snap.get("no_ask"),
                "spread": spread,
                "eligible": eligible,
                "reason_not_eligible": reason,
                "would_trade": would_trade,
                "side": side,
                "ev_raw": ev,
                "ev_net": ev,
                "cost_buffer": None,
                "raw_json": json.dumps(metadata),
            }

        def evaluate_side(side: str) -> tuple[dict[str, Any] | None, str | None]:
            if global_reasons:
                return None, ",".join(global_reasons)
            if prob_yes is None:
                return None, "missing_prob"
            if side == "YES":
                if not _ask_tradable(
                    yes_ask, config.min_ask_cents, config.max_ask_cents
                ):
                    counters["missing_yes_ask"] += 1
                    return None, "missing_yes_ask"
                ev = _ev_take_yes(prob_yes, yes_ask)
                row = build_row(side="YES", ev=ev, ask=yes_ask, reason=None)
                return row, None
            if side == "NO":
                if not _ask_tradable(
                    no_ask, config.min_ask_cents, config.max_ask_cents
                ):
                    counters["missing_no_ask"] += 1
                    return None, "missing_no_ask"
                ev = _ev_take_no(prob_yes, no_ask)
                row = build_row(side="NO", ev=ev, ask=no_ask, reason=None)
                return row, None
            return None, "invalid_side"

        evaluated: list[tuple[dict[str, Any] | None, str | None, str]] = []
        for side in ("YES", "NO"):
            row, reason = evaluate_side(side)
            evaluated.append((row, reason, side))

        if config.best_side_only:
            best_row = None
            best_ev = None
            best_reason = None
            best_side = None
            for row, reason, side in evaluated:
                if row is None:
                    if best_row is None and best_reason is None:
                        best_reason = reason
                        best_side = side
                    continue
                ev_val = row["ev_raw"]
                if best_row is None or (ev_val is not None and ev_val > best_ev):
                    best_row = row
                    best_ev = ev_val
                    best_reason = reason
                    best_side = side
            if best_row is not None:
                if best_ev is not None and best_ev >= config.min_ev:
                    take_rows.append(best_row)
                elif config.emit_passes:
                    pass_rows.append(
                        build_row(
                            side=best_side or "YES",
                            ev=best_ev,
                            ask=best_row.get(
                                "best_yes_ask"
                                if best_side == "YES"
                                else "best_no_ask"
                            ),
                            reason=best_reason or "ev_below_threshold",
                        )
                    )
            else:
                if config.emit_passes and best_reason is not None:
                    pass_rows.append(
                        build_row(
                            side=best_side or "YES",
                            ev=None,
                            ask=None,
                            reason=best_reason,
                        )
                    )
        else:
            for row, reason, side in evaluated:
                if row is not None and row["ev_raw"] is not None:
                    if row["ev_raw"] >= config.min_ev:
                        take_rows.append(row)
                    elif config.emit_passes:
                        row["eligible"] = 1
                        row["would_trade"] = 0
                        row["reason_not_eligible"] = "ev_below_threshold"
                        row["raw_json"] = json.dumps(
                            {
                                **json.loads(row["raw_json"]),
                                "decision": "PASS",
                                "decision_reason": "ev_below_threshold",
                            }
                        )
                        pass_rows.append(row)
                elif config.emit_passes:
                    pass_rows.append(
                        build_row(
                            side=side,
                            ev=None,
                            ask=None,
                            reason=reason or "missing_quote",
                        )
                    )

    if config.top_n is not None and config.top_n > 0:
        take_rows.sort(key=lambda r: r.get("ev_raw") or 0.0, reverse=True)
        kept = take_rows[: config.top_n]
        dropped = take_rows[config.top_n :]
        take_rows = kept
        if config.emit_passes and dropped:
            for row in dropped:
                counters["top_n_cutoff"] += 1
                row["would_trade"] = 0
                row["reason_not_eligible"] = "top_n_cutoff"
                row["raw_json"] = json.dumps(
                    {
                        **json.loads(row["raw_json"]),
                        "decision": "PASS",
                        "decision_reason": "top_n_cutoff",
                    }
                )
                pass_rows.append(row)

    counters["takes"] = len(take_rows)
    counters["passes"] = len(pass_rows)

    if config.emit_passes:
        rows.extend(take_rows + pass_rows)
    else:
        rows.extend(take_rows)
    return rows, counters
