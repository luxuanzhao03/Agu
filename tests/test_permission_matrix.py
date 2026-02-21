from trading_assistant.core.security import permission_matrix


def test_permission_matrix_contains_core_domains() -> None:
    pm = permission_matrix()
    assert "signal_generation" in pm
    assert "audit_read_export" in pm
    assert "data_license_governance" in pm
    assert "event_governance" in pm
    assert "alerts_subscription" in pm
    assert "ops_jobs" in pm
    assert "ops_scheduler" in pm
    assert "ops_dashboard" in pm
