# Adapter API (Python modules → `/api/*`)

This folder adds a **thin HTTP adapter** on top of the existing Python modules so the React dashboard can call `/api/*`.

It **does not modify** any existing project code; it only imports and orchestrates the current modules.

## Run

From repo root:

```powershell
python -m venv .venv_adapter
.\.venv_adapter\Scripts\pip install -r adapter_api\requirements.txt
.\.venv_adapter\Scripts\python -m uvicorn adapter_api.app:app --host 0.0.0.0 --port 8000
```

## OpenRouter (Executive Summary AI brief)

1) Copy `adapter_api/.env.example` to `adapter_api/.env`
2) Set `OPENROUTER_API_KEY`
3) (Optional) set `OPENROUTER_MODEL`

The React page calls:
- `GET /api/executive-llm-summary`

## Endpoints (used by the React UI)

- `GET /api/health`
- `GET /api/report/pdf`
- `GET /api/contract-status`
- `GET /api/blame-chain?dataset=week3_extractions`
- `GET /api/schema-diff?contract=generated_contracts/week3_extractions.yaml`
- `GET /api/ai-drift`
- `GET /api/llm-violations`
- `GET /api/executive-llm-summary`

## Guided Demo (6 steps)

- `POST /generate-contract`
- `POST /run-validation`
- `POST /run-attribution`
- `POST /schema-evolution`
- `POST /ai-extensions`
- `POST /generate-report`
