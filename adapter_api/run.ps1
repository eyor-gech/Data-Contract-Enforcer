param(
  [int]$Port = 8000,
  [string]$Host = "0.0.0.0"
)

$ErrorActionPreference = "Stop"

python -m uvicorn adapter_api.app:app --host $Host --port $Port

