class TradingExecutionBlockedError(PermissionError):
    """Raised when code attempts to execute real trades in this semi-auto system."""


def block_auto_execution() -> None:
    """
    Semi-auto boundary:
    This project only outputs research signals and trade-prep sheets.
    Real order placement must be done manually by the user.
    """
    raise TradingExecutionBlockedError(
        "Automatic order execution is disabled by design. Use manual broker workflow instead."
    )

