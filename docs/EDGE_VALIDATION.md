⸻


# EDGE_VALIDATION.md

## Purpose

This document defines **exactly what data must be logged**, **why it is logged**, and **how it will be analyzed** to determine whether the trading system has **real positive expected value** or is just noise.

This is not optional.  
If these checks are not passed, the strategy is considered **invalid** and trading should stop.

---

## Core Principle

A Kalshi threshold contract is a probability bet.

You only have edge if:
- Your probability estimates are **more accurate than the market’s**
- The difference is **large enough to overcome fees, slippage, and noise**
- This superiority is **statistically demonstrable**

PnL alone is insufficient.  
Calibration and likelihood tests are mandatory.

---

## What Must Be Logged (Non-Negotiable)

### 1. One row per opportunity (even if no trade is placed)

You must log **every market you evaluate**, not just trades.

This avoids survivorship bias.

#### Table: `opportunities`

| Field | Description |
|-----|------------|
| ts_eval | Timestamp of evaluation (UTC) |
| market_id | Kalshi market identifier |
| settlement_ts | Settlement time |
| strike | Threshold K |
| spot_price | Spot BTC price used |
| sigma | Volatility estimate used |
| tau | Time to settlement (years) |
| p_model | Model probability of YES |
| p_market | Market-implied probability |
| best_yes_bid | Derived best YES bid |
| best_yes_ask | Derived best YES ask |
| spread | Ask − bid |
| eligible | Boolean (passed universe filters) |
| reason_not_eligible | If not eligible, why |
| would_trade | Boolean (EV gate passed) |
| side | YES / NO / NONE |
| ev_raw | p_model − p_market |
| ev_net | p_model − (price + cost_buffer) |
| cost_buffer | Fees + slippage buffer assumed |

**Key rule:**  
This table is written **before** any order is sent.

---

### 2. Orders (what you attempted)

#### Table: `orders`

| Field | Description |
|-----|------------|
| order_id | Kalshi order id |
| client_order_id | Your id |
| market_id | Market |
| ts_sent | Timestamp |
| side | YES / NO |
| limit_price | Limit price |
| qty | Quantity |
| linked_opportunity_id | FK to opportunities |
| cancelled | Boolean |
| cancel_reason | If cancelled |

---

### 3. Fills (what actually happened)

#### Table: `fills`

| Field | Description |
|-----|------------|
| fill_id | Kalshi fill id |
| order_id | Order id |
| market_id | Market |
| ts_fill | Timestamp |
| side | YES / NO |
| price | Fill price |
| qty | Quantity |
| fee | Fee charged |

---

### 4. Settlements (ground truth)

#### Table: `settlements`

| Field | Description |
|-----|------------|
| market_id | Market |
| outcome | 1 = YES, 0 = NO |
| settlement_price | Settlement reference price |
| ts_settlement | Timestamp |

---

## Derived Fields (Computed Post-Hoc)

For each opportunity with a realized outcome:

- `y = outcome ∈ {0,1}`
- `error_model = (p_model − y)^2`
- `error_market = (p_market − y)^2`
- `logloss_model = −[y log(p_model) + (1−y) log(1−p_model)]`
- `logloss_market = −[y log(p_market) + (1−y) log(1−p_market)]`

---

## Calibration Analysis (Primary Test)

### Step 1: Bin probabilities

Bins:
- [0.0–0.1), [0.1–0.2), …, [0.9–1.0]

For each bin compute:
- Mean predicted probability
- Empirical frequency of YES

Do this **twice**:
- Once for `p_model`
- Once for `p_market`

---

### Step 2: Calibration plot

Plot:
- x-axis: mean predicted probability
- y-axis: empirical frequency
- Reference: y = x

**Pass condition:**
- Model curve is closer to diagonal than market curve in most bins

---

## Brier Score Test (Quadratic Accuracy)

Compute:

Brier_model  = mean((p_model  − y)^2)
Brier_market = mean((p_market − y)^2)

### Hypothesis test

Define:

d_i = error_market_i − error_model_i

Null hypothesis:

E[d_i] ≤ 0

Alternative:

E[d_i] > 0

Use:
- Bootstrap confidence interval (preferred)
- Or paired t-test

**Pass condition:**
- Mean(d_i) > 0 with statistical confidence

---

## Log Loss Test (Information Content)

Compute:

LogLoss_model
LogLoss_market
Δ = LogLoss_market − LogLoss_model

Interpretation:
- Δ > 0 ⇒ your probabilities explain reality better

This is the **strongest single test**.

**Pass condition:**
- Δ > 0 consistently across samples
- Not driven by a single outlier bin

---

## EV Attribution Test (Economic Relevance)

For each opportunity:

EV_model_i = p_model − p_market

Group opportunities into EV bins:
- Low EV
- Medium EV
- High EV

Compute:
- Average realized PnL per bin
- Cumulative realized PnL vs cumulative predicted EV

**Pass condition:**
- Higher predicted EV bins produce higher realized returns
- Relationship is monotonic, not random

---

## Out-of-Sample Validation

Split chronologically:
- First 50%: parameter selection
- Second 50%: frozen parameters

Repeat:
- Calibration
- Brier
- Log loss
- EV bin analysis

**If performance collapses out-of-sample → no edge.**

---

## Minimum Sample Sizes

- Calibration curves: ≥ 200 settled opportunities
- Hypothesis tests: ≥ 300 preferred
- Fewer acceptable only if mispricing magnitude is large

Before this, results are **indicative only**.

---

## Stop Conditions (Mandatory)

Trading must stop or be reduced if:
- Brier_model ≥ Brier_market
- LogLoss_model ≥ LogLoss_market
- High-EV bins do not outperform low-EV bins
- Calibration drifts materially over time
- Realized slippage > assumed cost buffer

No discretion. No “bad luck” rationalization.

---

## Why This Works

Most participants:
- Do not log untraded opportunities
- Do not measure calibration
- Do not compare against market probabilities
- Only look at PnL

This framework directly tests:
> “Are my probabilities more accurate than the market’s?”

If yes, positive EV follows mechanically once costs are handled.

---

## Final Rule

**If you cannot prove edge in this framework, you do not have edge.**

This document exists to prevent self-deception.


⸻