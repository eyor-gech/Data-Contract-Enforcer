# Data Contract Enforcer — React Dashboard

This folder contains a standalone React (TypeScript) executive dashboard that calls the existing backend APIs under `/api/*`.

## Run (development)

1) Install deps:

```bash
npm install
```

2) Start the adapter API (wraps the existing Python modules):

```powershell
python -m venv .venv_adapter
.\.venv_adapter\Scripts\pip install -r adapter_api\requirements.txt
.\.venv_adapter\Scripts\python -m uvicorn adapter_api.app:app --host 0.0.0.0 --port 8000
```

Optional (LLM Executive Brief):
- Create `C:\Users\Eyor.G\Documents\Tenx\Data-Contract-Enforcer\adapter_api\.env` from `adapter_api\.env.example`
- Set `OPENROUTER_API_KEY`

3) Start the UI (with an API proxy target):

```bash
set VITE_API_PROXY_TARGET=http://localhost:8000
npm run dev
```

The UI will call endpoints like:
- `GET /api/health`
- `GET /api/contract-status`
- `GET /api/blame-chain`
- `GET /api/schema-diff`
- `GET /api/ai-drift`
- `GET /api/llm-violations`

## Production

Recommended: serve the built static UI behind the same origin as the backend, so `/api/*` requests work without CORS.

```bash
npm run build
npm run preview
```
