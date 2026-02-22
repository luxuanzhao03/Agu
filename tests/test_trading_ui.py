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
    assert "/market/bars" in resp.text
    assert "/portfolio/rebalance/plan" in resp.text

    js = client.get("/ui/trading-workbench/app.js")
    assert js.status_code == 200
    assert "runSignal" in js.text
    assert "loadMarketBars" in js.text
    assert "runRebalancePlan" in js.text


def test_ui_main_portal_route() -> None:
    client = TestClient(app)
    resp = client.get("/ui/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")
    assert 'href="/trading/workbench"' in resp.text
    assert 'href="/ops/dashboard"' in resp.text


def test_ops_dashboard_has_cross_links() -> None:
    client = TestClient(app)
    resp = client.get("/ops/dashboard")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")
    assert 'href="/ui/"' in resp.text
    assert 'href="/trading/workbench"' in resp.text
