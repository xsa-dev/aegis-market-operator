from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

APP_DIR = Path(__file__).resolve().parent
DB_PATH = APP_DIR / "audit.db"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = db()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS audit (
              id TEXT PRIMARY KEY,
              created_at TEXT NOT NULL,
              symbol TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              result_json TEXT NOT NULL
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


class DemoRunRequest(BaseModel):
    symbol: str = Field(default="BTCUSDT")
    price: float = Field(default=68000, ge=0)
    funding_rate: float = Field(default=0.0001)

    # Risk/state inputs (demo knobs)
    top2_concentration: float = Field(default=0.42, ge=0, le=1)
    dd: float = Field(default=0.08, ge=0, le=1)
    fee_drag: float = Field(default=0.12, ge=0, le=1)


Decision = Literal["SCALE", "OBSERVE", "DISABLE"]


class DecisionMemo(BaseModel):
    summary: str
    rationale: list[str]
    proposed_action: str
    confidence: float = Field(ge=0, le=1)


class RiskGate(BaseModel):
    decision: Decision
    reasons: list[str]
    limits: dict


class ExecutionPlan(BaseModel):
    mode: Literal["paper", "disabled"]
    orders: list[dict]
    notes: list[str]


class SettlementRecord(BaseModel):
    rail: Literal["Arc"]
    asset: Literal["USDC"]
    status: Literal["mock_submitted", "skipped"]
    settlement_id: str


class DemoRunResult(BaseModel):
    audit_id: str
    created_at: str

    input: DemoRunRequest

    signal: dict
    memo: DecisionMemo
    risk: RiskGate
    execution: ExecutionPlan
    settlement: SettlementRecord


INDEX_HTML = (APP_DIR / "static" / "index.html").read_text(encoding="utf-8")

app = FastAPI(title="Agora Market Operator — Demo")


@app.on_event("startup")
def _startup() -> None:
    init_db()


@app.get("/health")
def health() -> dict:
    return {"ok": True, "time": utc_now_iso()}


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return INDEX_HTML


def make_signal(req: DemoRunRequest) -> dict:
    # Minimal, deterministic "signal" so the demo is stable.
    # You can replace this later with real market/onchain ingestion.
    signal_strength = 0.5
    if req.funding_rate < -0.0002:
        signal_strength += 0.2
    if req.funding_rate > 0.0004:
        signal_strength -= 0.2

    return {
        "symbol": req.symbol,
        "price": req.price,
        "funding_rate": req.funding_rate,
        "signal_strength": max(0.0, min(1.0, signal_strength)),
        "source": "mock_market_feed",
    }


def make_memo(req: DemoRunRequest, signal: dict) -> DecisionMemo:
    strength = float(signal["signal_strength"])

    proposed = "Hold"
    rationale = [
        f"Signal strength={strength:.2f} from mock market feed",
        f"Funding rate={req.funding_rate:.6f}",
    ]

    if strength >= 0.65:
        proposed = "Increase exposure (small size)"
        rationale.append("Signal above threshold for scaling")
    elif strength <= 0.35:
        proposed = "Reduce / stay flat"
        rationale.append("Signal below threshold; avoid forcing trades")
    else:
        rationale.append("Signal inconclusive; defaulting to Observe")

    # Confidence intentionally conservative.
    confidence = 0.55 + (strength - 0.5) * 0.3
    confidence = max(0.1, min(0.9, confidence))

    return DecisionMemo(
        summary="Decision memo generated (deterministic demo mode).",
        rationale=rationale,
        proposed_action=proposed,
        confidence=confidence,
    )


def risk_gate(req: DemoRunRequest, memo: DecisionMemo) -> RiskGate:
    reasons: list[str] = []

    # Guardrails from PRD (demo)
    limits = {
        "dd_disable": 0.20,
        "sharpe_observe": 1.0,  # not computed in demo
        "top2_concentration_reduce": 0.50,
        "fee_drag_pause": 0.30,
    }

    # We don't compute Sharpe in the demo, so we don't use that guardrail.
    if req.dd > limits["dd_disable"]:
        return RiskGate(decision="DISABLE", reasons=["DD > 20%"], limits=limits)

    if req.fee_drag > limits["fee_drag_pause"]:
        reasons.append("Fees > 30% of gross edge (fee_drag)")

    if req.top2_concentration > limits["top2_concentration_reduce"]:
        reasons.append("Top-2 concentration > 50%")

    # Final decision logic
    if reasons:
        return RiskGate(decision="OBSERVE", reasons=reasons, limits=limits)

    # If memo proposes scaling and no risk red flags -> SCALE, else OBSERVE
    if "Increase exposure" in memo.proposed_action:
        return RiskGate(decision="SCALE", reasons=["Within demo limits"], limits=limits)

    return RiskGate(decision="OBSERVE", reasons=["No strong signal"], limits=limits)


def execution_plan(req: DemoRunRequest, risk: RiskGate) -> ExecutionPlan:
    if risk.decision != "SCALE":
        return ExecutionPlan(mode="disabled", orders=[], notes=["Execution disabled by policy"])

    # Paper-trading style plan
    # Keep it tiny and safe.
    size_usd = 250
    return ExecutionPlan(
        mode="paper",
        orders=[
            {
                "venue": "mock_venue",
                "symbol": req.symbol,
                "side": "BUY",
                "type": "MARKET",
                "notional_usd": size_usd,
            }
        ],
        notes=["Paper execution only (demo)", "Replace with venue adapter later"],
    )


def settlement_record(risk: RiskGate) -> SettlementRecord:
    if risk.decision != "SCALE":
        return SettlementRecord(
            rail="Arc",
            asset="USDC",
            status="skipped",
            settlement_id="mock_skipped",
        )

    # Mock record shaped like a settlement object
    return SettlementRecord(
        rail="Arc",
        asset="USDC",
        status="mock_submitted",
        settlement_id=f"arc_mock_{uuid.uuid4().hex[:12]}",
    )


def save_audit(audit_id: str, symbol: str, payload: dict, result: dict) -> None:
    conn = db()
    try:
        conn.execute(
            "INSERT INTO audit (id, created_at, symbol, payload_json, result_json) VALUES (?, ?, ?, ?, ?)",
            (audit_id, utc_now_iso(), symbol, json.dumps(payload), json.dumps(result)),
        )
        conn.commit()
    finally:
        conn.close()


def load_audit(audit_id: str) -> dict:
    conn = db()
    try:
        row = conn.execute("SELECT * FROM audit WHERE id = ?", (audit_id,)).fetchone()
        if not row:
            raise KeyError(audit_id)
        return {
            "id": row["id"],
            "created_at": row["created_at"],
            "symbol": row["symbol"],
            "payload": json.loads(row["payload_json"]),
            "result": json.loads(row["result_json"]),
        }
    finally:
        conn.close()


@app.post("/api/run_demo", response_model=DemoRunResult)
def run_demo(req: DemoRunRequest) -> DemoRunResult:
    signal = make_signal(req)
    memo = make_memo(req, signal)
    risk = risk_gate(req, memo)
    execution = execution_plan(req, risk)
    settlement = settlement_record(risk)

    audit_id = uuid.uuid4().hex

    result = DemoRunResult(
        audit_id=audit_id,
        created_at=utc_now_iso(),
        input=req,
        signal=signal,
        memo=memo,
        risk=risk,
        execution=execution,
        settlement=settlement,
    )

    save_audit(audit_id, req.symbol, req.model_dump(), result.model_dump())
    return result


@app.get("/api/audit/{audit_id}")
def get_audit(audit_id: str) -> dict:
    try:
        return load_audit(audit_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="audit_id not found")


@app.get("/api/audits")
def list_audits(limit: int = 20) -> dict:
    limit = max(1, min(200, int(limit)))
    conn = db()
    try:
        rows = conn.execute(
            "SELECT id, created_at, symbol FROM audit ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return {
            "items": [
                {"id": r["id"], "created_at": r["created_at"], "symbol": r["symbol"]}
                for r in rows
            ]
        }
    finally:
        conn.close()
