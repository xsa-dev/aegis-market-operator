# Agora Market Operator — Minimal Full-Stack Demo (Hackathon)

This is a **minimal end-to-end demo** of the concept described in `~/Agora_Agents_Hackathon/`:

**signal → reasoning → risk gate → execution → (mock) Arc/USDC settlement → audit trail**

No real exchange trading, no real Arc integration — everything is safe and mockable, but the *pipeline is real* and observable.

## What you get

- FastAPI backend with a single "run" endpoint that simulates the full pipeline
- SQLite audit log (every run is stored and retrievable)
- Simple static web UI (vanilla JS) to run the demo and display all steps

## Run locally (dev)

```bash
cd ~/agora-demo
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

Open: http://localhost:8000

## Run with Docker

```bash
cd ~/agora-demo
docker build -t agora-demo .
docker run --rm -p 8000:8000 agora-demo
```

## API

- `GET /health`
- `POST /api/run_demo`
- `GET /api/audit/{audit_id}`

Example:
```bash
curl -s http://localhost:8000/api/run_demo \
  -H 'content-type: application/json' \
  -d '{"symbol":"BTCUSDT","price":68000,"funding_rate":0.0001,"top2_concentration":0.42,"dd":0.08,"fee_drag":0.12}' | jq
```

## Notes

- The "LLM reasoning" step is **deterministic** by default (safe for demos). You can later swap it for a real structured-output model.
- The "Settlement" step writes a mock record shaped like an Arc/USDC settlement.
