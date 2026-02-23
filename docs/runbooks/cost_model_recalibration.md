# Cost Model Recalibration Runbook

## Goal
- Re-estimate execution cost assumptions (`slippage_rate`, `impact_cost_coeff`, `fill_probability_floor`) from manual replay records.
- Reduce optimistic backtest bias and keep production assumptions synchronized with real execution quality.

## Inputs
- Replay signal/execution pairs in `replay.db`.
- Execution records should include `reference_price` for slippage coverage.
- Recommended minimum sample:
  - `sample_size >= 50`
  - `executed_samples >= 30`
  - `slippage_coverage >= 0.4`

## Trigger Paths
- API:
  - `POST /replay/cost-model/calibrate`
  - `GET /replay/cost-model/calibrations`
- Scheduled job:
  - `job_type=cost_model_recalibration`
  - example payload: `docs/examples/job_register_cost_model_recalibration.json`
- Workbench:
  - Execution tab -> `成本模型重估`

## Validation Checklist
- Check calibration confidence:
  - `confidence >= 0.6` for direct adoption.
  - `0.4 <= confidence < 0.6` for gray rollout only.
  - `< 0.4` keep previous defaults and collect more data.
- Check stability:
  - `p90_abs_slippage_bps` should not spike > 2x previous week.
  - no-execution ratio should be trending down.
- Check coverage:
  - if `slippage_coverage < 0.4`, enforce filling `reference_price` in execution writeback.

## Rollout Pattern
1. Apply recommended parameters only to gray scope:
   - by strategy and/or by symbol.
2. Observe for 7-20 trading days:
   - follow rate, drawdown, cost-adjusted return.
3. If underperforming baseline, rollback to previous profile.

## Failure Handling
- If calibration API/job fails:
  - retry once (5-10s backoff).
  - if still failing, create critical alert with payload snapshot.
- If calibration sample is insufficient:
  - do not update parameters.
  - schedule next run after additional execution records.

## Audit / Evidence
- Each calibration run produces:
  - calibration record (`/replay/cost-model/calibrations`)
  - optional cost-model report file under `reports/`
  - audit event: `event_type=cost_model`, `action=calibrate`
- Include these artifacts in compliance evidence bundles for change traceability.
