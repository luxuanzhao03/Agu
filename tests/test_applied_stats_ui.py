from fastapi.testclient import TestClient

from trading_assistant.main import app


def test_applied_stats_showcase_route() -> None:
    client = TestClient(app)
    resp = client.get("/applied-stats/showcase")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")
    assert "marketFactorStudyForm" in resp.text
    assert "runMarketFactorStudyBtn" in resp.text
    assert "runDescriptiveDemoBtn" in resp.text
    assert 'href="/ui/"' in resp.text
    assert 'href="/trading/workbench"' in resp.text
    assert 'href="/ops/dashboard"' in resp.text


def test_applied_stats_showcase_static_assets() -> None:
    client = TestClient(app)

    css = client.get("/ui/applied-stats-showcase/styles.css")
    assert css.status_code == 200
    assert "text/css" in css.headers.get("content-type", "")
    assert ".hero" in css.text
    assert "#resultViewer" in css.text

    js = client.get("/ui/applied-stats-showcase/app.js")
    assert js.status_code == 200
    assert "runMarketFactorStudy" in js.text
    assert "runDescriptiveDemo" in js.text
    assert "/applied-stats/cases/market-factor-study" in js.text

