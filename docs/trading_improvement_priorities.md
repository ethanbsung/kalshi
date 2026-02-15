# Trading Improvement Priorities

Last updated: 2026-02-15

## Current Read

- The strategy can produce wins, but downside tails are too large.
- Biggest drag appears to be expensive entries (high cents paid) plus holding losers to near-max loss.
- Model-vs-market quality is still weak enough that risk controls should come before major model refactors.

## Highest-ROI Changes (Do 1-3 Now)

## 1) Add model-aware exit logic (highest ROI)

Implement an exit check for every open position:

- Compute `hold_value_cents` from the latest model probability:
  - YES position: `p_now * 100`
  - NO position: `(1 - p_now) * 100`
- Compare to current liquidation value (`liqC`).
- Exit if `hold_value_cents < liqC + edge_buffer_cents`.

Recommended starting params:

- `edge_buffer_cents = 2` to `4`
- Require condition to hold for 2 consecutive checks before exiting (reduces churn).

Why first:

- Cuts tail losses without blindly stopping out all drawdowns.
- Uses current information instead of anchoring to entry price.

## 2) Add entry price guardrails

For now, block most new entries in extreme price zones unless edge is very strong.

Recommended starting rule:

- Skip entries above `85c` or below `15c` unless expected edge exceeds a stronger threshold.

Why second:

- Many large losses come from poor asymmetry at expensive prices.
- Simple rule, easy to test, immediate impact on worst-case outcomes.

## 3) Size by max loss (not fixed contracts)

Use a fixed dollar risk budget per trade:

- `position_size = floor(risk_budget_per_trade / max_loss_per_contract)`
- For YES: `max_loss_per_contract = entry_price`
- For NO: `max_loss_per_contract = entry_price`

Why third:

- Prevents high-price contracts from dominating drawdowns.
- Improves survival while edge is still uncertain.

## Rollout Strategy

Do not wait for thousands of trades before any changes.  
Risk controls should be added now because they protect capital during data collection.

But do not ship many changes at once.

Recommended cadence:

1. Ship change #1 only; run for 200-400 settled trades.
2. Review metrics (pnl, tail loss, avg loss, realized vs unrealized drift).
3. If improved or neutral, add change #2; repeat.
4. Then add change #3.

## What to Track After Each Change

- `realized_pnl`
- Average win, average loss
- 90th/95th percentile worst-trade loss
- Count of losses worse than `-0.60`
- Fraction of trades entered above `85c` or below `15c`
- Exit reason counts:
  - model edge decay exit
  - time-based exit
  - settlement exit

## Practical Notes

- Prefer model-aware exits over fixed stop-loss triggers in short-horizon markets.
- Use a time-to-settlement rule as a backup (for example, tighten exit threshold in the last 10-20 minutes).
- Revisit model calibration after risk controls stabilize execution outcomes.
