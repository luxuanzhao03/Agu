from fastapi.testclient import TestClient

from trading_assistant.main import app


def test_trading_workbench_ui_route() -> None:
    client = TestClient(app)
    resp = client.get("/trading/workbench")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")
    assert "marketKlineChart" in resp.text
    assert "targetWeightChart" in resp.text
    assert "rebalanceRows" in resp.text
    assert 'data-tab="autotune"' in resp.text
    assert 'data-tab="challenge"' in resp.text
    assert "runPortfolioBacktestBtn" in resp.text
    assert "rolloutRuleRows" in resp.text
    assert "runChallengeBtn" in resp.text
    assert "challengeResultRows" in resp.text
    assert "challengeDisableRiskControlsInput" in resp.text
    assert "challengeGateMinValidationSharpeInput" in resp.text
    assert "autotuneReturnVarWeightInput" in resp.text
    assert "portfolioRiskMaxDailyLossInput" in resp.text
    assert 'data-tab="datahub"' in resp.text
    assert "runDataFetchBarsBtn" in resp.text
    assert "runDataFetchIntradayBtn" in resp.text
    assert "runDataFetchCapabilitiesBtn" in resp.text
    assert "runDataFetchPrefetchBtn" in resp.text
    assert "runDataFetchAllBtn" in resp.text
    assert "runFullMarket2000ScanBtn" in resp.text
    assert "cancelFullMarket2000ScanBtn" in resp.text
    assert "dataFetchCapabilityRows" in resp.text
    assert "dataFetchPrefetchRows" in resp.text
    assert "fullMarket2000ScanRows" in resp.text
    assert "fullMarket2000SymbolTimeoutSecInput" in resp.text
    assert "fullMarket2000NetworkTimeoutSecInput" in resp.text
    assert "fullMarket2000KpiTimeouts" in resp.text
    assert 'data-tab="holdings"' in resp.text
    assert "submitHoldingTradeBtn" in resp.text
    assert "runHoldingAnalyzeBtn" in resp.text
    assert "holdingRecommendationRows" in resp.text
    assert "holdingIntradayIntervalInput" in resp.text
    assert "holdingIntradayLookbackInput" in resp.text
    assert "holdingTradeReferencePriceInput" in resp.text
    assert "holdingTradeExecutedAtInput" in resp.text
    assert "loadHoldingAccuracyBtn" in resp.text
    assert "holdingAccDetailRows" in resp.text
    assert "loadGoLiveReadinessBtn" in resp.text
    assert "goLiveGateRows" in resp.text
    assert "loadReplayAttributionBtn" in resp.text
    assert "closureReportPreview" in resp.text
    assert "runCostCalibrationBtn" in resp.text
    assert "costModelHistoryRows" in resp.text
    assert "/market/bars" in resp.text
    assert "/portfolio/rebalance/plan" in resp.text

    js = client.get("/ui/trading-workbench/app.js")
    assert js.status_code == 200
    assert "runSignal" in js.text
    assert "loadMarketBars" in js.text
    assert "runRebalancePlan" in js.text
    assert "runPortfolioBacktest" in js.text
    assert "rollbackAutotuneProfile" in js.text
    assert "loadRolloutRules" in js.text
    assert "runDataFetchAll" in js.text
    assert "buildFullMarket2000ScanRequest" in js.text
    assert "runFullMarket2000Scan" in js.text
    assert "cancelFullMarket2000Scan" in js.text
    assert "syncFullMarket2000ScanActionButtons" in js.text
    assert "renderFullMarket2000Scan" in js.text
    assert "loadFullMarket2000ScanProgress" in js.text
    assert "/research/full-market-2000-scan/progress" in js.text
    assert "/research/full-market-2000-scan/cancel" in js.text
    assert "loadDataFetchTushareCapabilities" in js.text
    assert "runDataFetchTusharePrefetch" in js.text
    assert "renderDataFetchWorkbench" in js.text
    assert "buildStrategyChallengeRequest" in js.text
    assert "disable_risk_controls" in js.text
    assert "runStrategyChallenge" in js.text
    assert "renderChallengeWorkbench" in js.text
    assert "applyChallengeChampionToStrategyForm" in js.text
    assert "submitHoldingTrade" in js.text
    assert "loadHoldingPositions" in js.text
    assert "runHoldingAnalyze" in js.text
    assert "buildHoldingAccuracyQuery" in js.text
    assert "loadHoldingAccuracyReport" in js.text
    assert "renderHoldingAccuracy" in js.text
    assert "buildGoLiveReadinessQuery" in js.text
    assert "loadGoLiveReadinessReport" in js.text
    assert "renderGoLiveReadiness" in js.text
    assert "loadReplayAttribution" in js.text
    assert "generateClosureReport" in js.text
    assert "runCostCalibration" in js.text
    assert "loadCostCalibrationHistory" in js.text


def test_ui_main_portal_route() -> None:
    client = TestClient(app)
    resp = client.get("/ui/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")
    assert 'href="/trading/workbench"' in resp.text
    assert 'href="/ops/dashboard"' in resp.text
    assert 'href="/applied-stats/showcase"' in resp.text


def test_ops_dashboard_has_cross_links() -> None:
    client = TestClient(app)
    resp = client.get("/ops/dashboard")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")
    assert 'href="/ui/"' in resp.text
    assert 'href="/trading/workbench"' in resp.text
