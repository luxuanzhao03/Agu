from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from trading_assistant.core.models import AutoTuneApplyScope, AutoTuneProfileRecord, AutoTuneRolloutRuleRecord


class AutoTuneStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS autotune_profiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    strategy_name TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    strategy_params TEXT NOT NULL,
                    objective_score REAL NOT NULL,
                    validation_total_return REAL,
                    source_run_id TEXT NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1,
                    note TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_autotune_lookup
                ON autotune_profiles(strategy_name, scope, symbol, active, id DESC)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS autotune_rollout_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    strategy_name TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    note TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_autotune_rollout_unique
                ON autotune_rollout_rules(strategy_name, symbol)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_autotune_rollout_lookup
                ON autotune_rollout_rules(strategy_name, symbol, id DESC)
                """
            )

    def upsert_active_profile(
        self,
        *,
        strategy_name: str,
        scope: AutoTuneApplyScope,
        symbol: str | None,
        strategy_params: dict[str, float | int | str | bool],
        objective_score: float,
        validation_total_return: float | None,
        source_run_id: str,
        note: str = "",
    ) -> AutoTuneProfileRecord:
        now = datetime.now(timezone.utc).isoformat()
        symbol_key = self._normalize_symbol_key(symbol if scope == AutoTuneApplyScope.SYMBOL else None)
        params_json = json.dumps(self._normalize_params(strategy_params), ensure_ascii=True, sort_keys=True)
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE autotune_profiles
                SET active = 0, updated_at = ?
                WHERE strategy_name = ? AND scope = ? AND symbol = ? AND active = 1
                """,
                (now, strategy_name, scope.value, symbol_key),
            )
            cur = conn.execute(
                """
                INSERT INTO autotune_profiles(
                    created_at, updated_at, strategy_name, scope, symbol, strategy_params,
                    objective_score, validation_total_return, source_run_id, active, note
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                """,
                (
                    now,
                    now,
                    strategy_name,
                    scope.value,
                    symbol_key,
                    params_json,
                    float(objective_score),
                    float(validation_total_return) if validation_total_return is not None else None,
                    source_run_id,
                    note,
                ),
            )
            row_id = int(cur.lastrowid)
            row = conn.execute(
                """
                SELECT id, created_at, updated_at, strategy_name, scope, symbol, strategy_params,
                       objective_score, validation_total_return, source_run_id, active, note
                FROM autotune_profiles
                WHERE id = ?
                """,
                (row_id,),
            ).fetchone()
        if row is None:
            raise RuntimeError("failed to load inserted autotune profile")
        return self._to_record(row)

    def get_profile(self, profile_id: int) -> AutoTuneProfileRecord | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, created_at, updated_at, strategy_name, scope, symbol, strategy_params,
                       objective_score, validation_total_return, source_run_id, active, note
                FROM autotune_profiles
                WHERE id = ?
                LIMIT 1
                """,
                (profile_id,),
            ).fetchone()
        return self._to_record(row) if row else None

    def list_profiles(
        self,
        *,
        strategy_name: str | None = None,
        symbol: str | None = None,
        active_only: bool = False,
        limit: int = 200,
    ) -> list[AutoTuneProfileRecord]:
        sql = """
            SELECT id, created_at, updated_at, strategy_name, scope, symbol, strategy_params,
                   objective_score, validation_total_return, source_run_id, active, note
            FROM autotune_profiles
            WHERE 1 = 1
        """
        params: list[Any] = []
        if strategy_name:
            sql += " AND strategy_name = ?"
            params.append(strategy_name)
        if symbol:
            key = self._normalize_symbol_key(symbol)
            sql += " AND (symbol = ? OR (scope = ? AND symbol = ''))"
            params.extend([key, AutoTuneApplyScope.GLOBAL.value])
        if active_only:
            sql += " AND active = 1"
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(max(1, min(int(limit), 5000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_record(row) for row in rows]

    def get_active_profile(self, *, strategy_name: str, symbol: str | None = None) -> AutoTuneProfileRecord | None:
        symbol_key = self._normalize_symbol_key(symbol)
        with self._conn() as conn:
            if symbol_key:
                row = conn.execute(
                    """
                    SELECT id, created_at, updated_at, strategy_name, scope, symbol, strategy_params,
                           objective_score, validation_total_return, source_run_id, active, note
                    FROM autotune_profiles
                    WHERE strategy_name = ? AND scope = ? AND symbol = ? AND active = 1
                    ORDER BY id DESC LIMIT 1
                    """,
                    (strategy_name, AutoTuneApplyScope.SYMBOL.value, symbol_key),
                ).fetchone()
                if row:
                    return self._to_record(row)

            row = conn.execute(
                """
                SELECT id, created_at, updated_at, strategy_name, scope, symbol, strategy_params,
                       objective_score, validation_total_return, source_run_id, active, note
                FROM autotune_profiles
                WHERE strategy_name = ? AND scope = ? AND symbol = '' AND active = 1
                ORDER BY id DESC LIMIT 1
                """,
                (strategy_name, AutoTuneApplyScope.GLOBAL.value),
            ).fetchone()
        return self._to_record(row) if row else None

    def activate_profile(self, profile_id: int) -> AutoTuneProfileRecord | None:
        row = self.get_profile(profile_id)
        if row is None:
            return None
        now = datetime.now(timezone.utc).isoformat()
        symbol_key = self._normalize_symbol_key(row.symbol if row.scope == AutoTuneApplyScope.SYMBOL else None)
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE autotune_profiles
                SET active = 0, updated_at = ?
                WHERE strategy_name = ? AND scope = ? AND symbol = ?
                """,
                (now, row.strategy_name, row.scope.value, symbol_key),
            )
            conn.execute(
                "UPDATE autotune_profiles SET active = 1, updated_at = ? WHERE id = ?",
                (now, profile_id),
            )
        return self.get_profile(profile_id)

    def rollback_active_profile(
        self,
        *,
        strategy_name: str,
        scope: AutoTuneApplyScope,
        symbol: str | None = None,
    ) -> AutoTuneProfileRecord | None:
        symbol_key = self._normalize_symbol_key(symbol if scope == AutoTuneApplyScope.SYMBOL else None)
        with self._conn() as conn:
            active = conn.execute(
                """
                SELECT id
                FROM autotune_profiles
                WHERE strategy_name = ? AND scope = ? AND symbol = ? AND active = 1
                ORDER BY id DESC
                LIMIT 1
                """,
                (strategy_name, scope.value, symbol_key),
            ).fetchone()
            if active is None:
                return None
            previous = conn.execute(
                """
                SELECT id
                FROM autotune_profiles
                WHERE strategy_name = ? AND scope = ? AND symbol = ? AND id < ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (strategy_name, scope.value, symbol_key, int(active["id"])),
            ).fetchone()
        if previous is None:
            return None
        return self.activate_profile(int(previous["id"]))

    def upsert_rollout_rule(
        self,
        *,
        strategy_name: str,
        symbol: str | None,
        enabled: bool,
        note: str = "",
    ) -> AutoTuneRolloutRuleRecord:
        now = datetime.now(timezone.utc).isoformat()
        symbol_key = self._normalize_symbol_key(symbol)
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT id
                FROM autotune_rollout_rules
                WHERE strategy_name = ? AND symbol = ?
                LIMIT 1
                """,
                (strategy_name, symbol_key),
            ).fetchone()
            if row is None:
                cur = conn.execute(
                    """
                    INSERT INTO autotune_rollout_rules(
                        created_at, updated_at, strategy_name, symbol, enabled, note
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (now, now, strategy_name, symbol_key, 1 if enabled else 0, note),
                )
                rule_id = int(cur.lastrowid)
            else:
                rule_id = int(row["id"])
                conn.execute(
                    """
                    UPDATE autotune_rollout_rules
                    SET updated_at = ?, enabled = ?, note = ?
                    WHERE id = ?
                    """,
                    (now, 1 if enabled else 0, note, rule_id),
                )
            saved = conn.execute(
                """
                SELECT id, created_at, updated_at, strategy_name, symbol, enabled, note
                FROM autotune_rollout_rules
                WHERE id = ?
                LIMIT 1
                """,
                (rule_id,),
            ).fetchone()
        if saved is None:
            raise RuntimeError("failed to persist rollout rule")
        return self._to_rollout_rule(saved)

    def list_rollout_rules(
        self,
        *,
        strategy_name: str | None = None,
        symbol: str | None = None,
        limit: int = 500,
    ) -> list[AutoTuneRolloutRuleRecord]:
        sql = """
            SELECT id, created_at, updated_at, strategy_name, symbol, enabled, note
            FROM autotune_rollout_rules
            WHERE 1 = 1
        """
        params: list[Any] = []
        if strategy_name:
            sql += " AND strategy_name = ?"
            params.append(strategy_name)
        if symbol is not None:
            sql += " AND symbol = ?"
            params.append(self._normalize_symbol_key(symbol))
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(max(1, min(int(limit), 5000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_rollout_rule(row) for row in rows]

    def get_rollout_rule(self, *, strategy_name: str, symbol: str | None = None) -> AutoTuneRolloutRuleRecord | None:
        symbol_key = self._normalize_symbol_key(symbol)
        with self._conn() as conn:
            if symbol_key:
                row = conn.execute(
                    """
                    SELECT id, created_at, updated_at, strategy_name, symbol, enabled, note
                    FROM autotune_rollout_rules
                    WHERE strategy_name = ? AND symbol = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (strategy_name, symbol_key),
                ).fetchone()
                if row is not None:
                    return self._to_rollout_rule(row)
            row = conn.execute(
                """
                SELECT id, created_at, updated_at, strategy_name, symbol, enabled, note
                FROM autotune_rollout_rules
                WHERE strategy_name = ? AND symbol = ''
                ORDER BY id DESC
                LIMIT 1
                """,
                (strategy_name,),
            ).fetchone()
        return self._to_rollout_rule(row) if row else None

    def delete_rollout_rule(self, rule_id: int) -> bool:
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM autotune_rollout_rules WHERE id = ?", (rule_id,))
        return cur.rowcount > 0

    @staticmethod
    def _normalize_symbol_key(symbol: str | None) -> str:
        if symbol is None:
            return ""
        key = str(symbol).strip().upper()
        return key

    @staticmethod
    def _normalize_params(params: dict[str, Any]) -> dict[str, float | int | str | bool]:
        out: dict[str, float | int | str | bool] = {}
        for key, value in params.items():
            if isinstance(value, bool):
                out[str(key)] = bool(value)
                continue
            if isinstance(value, int):
                out[str(key)] = int(value)
                continue
            if isinstance(value, float):
                out[str(key)] = float(value)
                continue
            if isinstance(value, str):
                out[str(key)] = value
                continue
            out[str(key)] = str(value)
        return out

    @staticmethod
    def _json_to_params(raw: str) -> dict[str, float | int | str | bool]:
        try:
            loaded = json.loads(raw)
        except Exception:  # noqa: BLE001
            return {}
        if not isinstance(loaded, dict):
            return {}
        return AutoTuneStore._normalize_params(loaded)

    def _to_record(self, row: sqlite3.Row) -> AutoTuneProfileRecord:
        symbol_value = str(row["symbol"])
        scope = AutoTuneApplyScope(str(row["scope"]))
        return AutoTuneProfileRecord(
            id=int(row["id"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
            strategy_name=str(row["strategy_name"]),
            scope=scope,
            symbol=(symbol_value if symbol_value else None),
            strategy_params=self._json_to_params(str(row["strategy_params"])),
            objective_score=float(row["objective_score"]),
            validation_total_return=(float(row["validation_total_return"]) if row["validation_total_return"] is not None else None),
            source_run_id=str(row["source_run_id"]),
            active=bool(int(row["active"])),
            note=str(row["note"]),
        )

    def _to_rollout_rule(self, row: sqlite3.Row) -> AutoTuneRolloutRuleRecord:
        symbol_value = str(row["symbol"])
        return AutoTuneRolloutRuleRecord(
            id=int(row["id"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
            strategy_name=str(row["strategy_name"]),
            symbol=(symbol_value if symbol_value else None),
            enabled=bool(int(row["enabled"])),
            note=str(row["note"]),
        )
