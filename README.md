# A2A Invoice Action Agent (Render + free hosted model)

This is a starter implementation for the assignment contract. It provides the requested HTTP+JSON routes, owner-scoped tasks, message idempotency, semantic package decision caching, proposal gating, and receipt binding.

## Important limitations before grading

- Use Render Postgres for `DATABASE_URL`. Render's free web service filesystem is ephemeral; SQLite on Render can lose task/cache state after restart.
- The model provider is configurable. Defaults target OpenRouter's free NVIDIA Nemotron Nano Omni reasoning model; create a free API key and set `LLM_API_KEY`. Free models can rate-limit or change availability. This endpoint does not enforce JSON mode, so the service validates the returned JSON and fails closed if it is malformed. Test the provider before using any grader Check.
- This implementation has not been tested against the assignment's hidden grader. Validate every requirement and run local tests first.
- Never put API keys in GitHub. Add them only as Render environment variables.
- The model is asked for exactly three bracketed evidence refs and a typed action per package. Invalid model output fails closed rather than fabricating a proposal.
- No payment is executed by this service. It only records accepted proposal receipts returned by the grader.
- Every distinct Bearer token is treated as a separate principal. Raw tokens are never stored.
- The initial request's `configuration` is intentionally excluded from the idempotency hash; the `message` object alone is hashed using recursively sorted compact JSON.

## Files

- `app.py`: FastAPI service.
- `requirements.txt`: runtime dependencies.
- `render.yaml`: Render Blueprint starter.
- `smoke_test.py`: quick local protocol check (no model call).

## Deploy

1. Create a public GitHub repository and upload `app.py`, `requirements.txt`, `render.yaml`, and `README.md`.
2. In Render, create a free **Postgres** database. Copy its **Internal Database URL** if the web service is in the same region, or the External URL if needed. Free Render Postgres databases expire after 30 days, so monitor the database lifecycle.
3. Create a Render Web Service from your GitHub repo (Python).
   - Build command: `pip install -r requirements.txt`
   - Start command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
   - Plan: Free
4. Add environment variables:
   - `DATABASE_URL` = your Render Postgres URL
   - `A2A_BASE_URL` = `https://YOUR-SERVICE.onrender.com/a2a/` (include `/a2a/` and trailing slash)
   - `LLM_API_KEY` = your OpenRouter API key
   - `LLM_BASE_URL` = `https://openrouter.ai/api/v1`
   - `LLM_MODEL` = `nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free`
5. Redeploy after setting variables.
6. Open `https://YOUR-SERVICE.onrender.com/.well-known/agent-card.json`. Verify `supportedInterfaces[0].url` exactly equals the base URL you plan to submit.
7. Submit the base URL `https://YOUR-SERVICE.onrender.com/a2a/` to the assignment.

## Get a free model key

1. Sign up at https://openrouter.ai/
2. Create an API key at https://openrouter.ai/keys
3. If the free model returns 429 or is unavailable, check current free model availability and limits. Do not use a grader check to test the model provider.

## Local run

```bash
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:DATABASE_URL="sqlite:///./local.db"
$env:A2A_BASE_URL="https://example.invalid/a2a/"
$env:LLM_API_KEY="YOUR_KEY"
uvicorn app:app --reload
```

## Protocol requirements implemented

- Public `GET /.well-known/agent-card.json`.
- Protected `POST /a2a/message:send`, `GET /a2a/tasks/{id}`, `GET /a2a/tasks`, `POST /a2a/tasks/{id}:cancel`.
- Requires `A2A-Version: 1.0`; body POSTs require `application/a2a+json`.
- `/message:send` returns `{"task": Task}`; task reads and cancellation return Task; listing returns `{"tasks":[...]}`.
- Initial batch task is `TASK_STATE_INPUT_REQUIRED` with a proposal artifact.
- Results continuation is bound to principal/task/context/batch/package/action/actionId and nonce.
- Only accepted results become executions in the receipts artifact; rejected proposals are never executed.
- Same principal + messageId + same message content replays the stored task; changed content returns `409 IDEMPOTENCY_CONFLICT`.
- Task reads/listing are owner-scoped.
- Task response size is bounded by the incoming request limit; keep generated artifacts below 512 KiB.
