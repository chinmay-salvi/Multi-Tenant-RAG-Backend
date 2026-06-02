# Backend Agents

Backend service for a no-code AI chatbot platform. Organizations build configurable chatbots grounded in custom knowledge (datafeeds), deploy them via widgets or managed channels, and handle conversations, leads, support tickets, and billing through a unified domain-driven FastAPI API.

## Features

* **Domain-Driven Modular Architecture** — Clean, scalable code structure separated by business domains (Chatbots, Messaging, Ticketing, Leads, Billing, Datafeeds).
* **Multi-tenant chatbots** — Organizations, users, tags, and per-chatbot configuration (purpose, company context, intro messages, branding).
* **Retrieval-augmented chat (RAG)** — Answers are grounded in embedded datafeeds stored in PostgreSQL with **pgvector**.
* **Non-Blocking Real-time Streaming** — Real-time assistant replies over **Server-Sent Events (SSE)** with subprocess metadata, utilizing non-blocking EAFP exception-based stream state handling.
* **Coherent Conversation Memory** — Full message history persisted per conversation; recent turns plus **query condensation** keep follow-up questions coherent in longer threads.
* **Enterprise Concurrency Protection** — Implements atomic parent counters (`lastTicketSequence` on `Organization`) ensuring gapless support ticket sequence numbers under concurrent threads.
* **Knowledge Ingestion** — Upload text/files, scrape URLs, and background workers for embedding and scraping pipelines, offloaded cleanly to asynchronous execution.
* **Managed Backend Integration** — Fully asynchronous webhook-style messaging for external inbox platforms using `httpx` for high-frequency concurrency, featuring automatic human handoff triggers.
* **Plans & Payments** — Active usage limit guardrails (chatbot counts, message volumes, datafeed token sizes) offloaded to threadpools (`asyncio.to_thread`) to prevent blocking the FastAPI event loop.

---

## Architecture

```
┌─────────────┐     ┌──────────────────────────────────────────────────┐
│   Client    │────▶│  FastAPI (nginx)  /api/v1/*                      │
│  (widget)   │     │  • Domain-Driven Modularity (app/*)              │
│             │     │  • Auth (Supabase JWT)                           │
│             │     │  • Async Webhook Messaging (httpx)               │
│             │     │  • Non-blocking limit verification               │
└─────────────┘     └───────────┬──────────────────────┬───────────────┘
                                │                      │
                    ┌───────────▼──────────┐  ┌────────▼────────────┐
                    │  PostgreSQL +        │  │  S3 (file uploads)  │
                    │  pgvector            │  │                     │
                    │  • app tables        │  └─────────────────────┘
                    │  • vector store      │
                    └───────────▲──────────┘
                                │
                    ┌───────────┴──────────┐
                    │  Datafeed workers    │
                    │  • embed (queue)     │
                    │  • scrape (URLs)     │
                    └──────────────────────┘
```

### Chat pipeline

1. User message is stored on the conversation.
2. Prior **successful** messages are loaded into a LlamaIndex `ChatMessage` history.
3. A **CondensePlusContextChatEngine** rewrites follow-ups into standalone questions and retrieves relevant chunks from the vector store (filtered by `orgId` and `chatbotId`).
4. The LLM responds with company-scoped instructions; tokens stream back over SSE.
5. Assistant message and subprocess events are persisted when the stream completes.

Supported LLM/embed paths include **Groq** and **OpenAI**, with **Cloudflare Workers AI** embeddings. Credentials for external APIs are rotated via `SysAuthCred` and `auth_creds_cycler`.

---

## Project Structure

The codebase is organized using a **modular, domain-driven architecture** where each folder under `app/` is self-contained with its own API routers, schemas, CRUD helpers, and business services.

```
backend-agents-main/
├── app/                          # Main FastAPI application
│   ├── chatbot/                  # Chatbot design, branding, and active configurations
│   ├── message/                  # Streaming engines (RAG), conversation history, managed inbox
│   ├── datafeed/                 # File ingestion, URL scraping, and raw knowledge loaders
│   ├── ticket/                   # Support ticketing (atomic sequence generation)
│   ├── lead/                     # Custom lead forms and captured contact templates
│   ├── payments/                 # Plans, billing limits, and payment validation (threadpool offloaded)
│   ├── user_org/                 # Multi-tenant registrations and rotated credential cyclers
│   ├── core/                     # Application configurations (config.py, dependencies)
│   ├── utils/                    # Shared utilities (S3 helpers, formatters)
│   ├── backend_app.py            # App entrypoint
│   └── requirements.txt
├── datafeed_processing/          # Standalone background embed & scrape workers
├── docker-compose.yml            # App + Postgres (pgvector) + nginx stack
├── datafeed-docker-compose.yml   # Embed and scrape worker services stack
└── commands.txt                  # Local dev quick reference
```

---

## Tech Stack

| Layer | Technology |
|--------|------------|
| **API Framework** | FastAPI, Uvicorn, SSE (`sse-starlette`) |
| **ORM / Database** | SQLAlchemy (async), PostgreSQL, pgvector |
| **RAG Engine** | LlamaIndex (CondensePlusContextChatEngine, VectorStoreIndex) |
| **Authentication** | Supabase JWT |
| **Outbound HTTP** | Non-blocking `httpx` AsyncClient (webhook dispatching) |
| **File Storage** | AWS S3 Integration |
| **Payments** | Razorpay webhooks |
| **Worker Queues** | PgQueuer push-based queue (with asynchronous LISTEN/NOTIFY and DLQ retry pipelines) |

---

## Prerequisites

- Python 3.11+
- PostgreSQL 16 with **pgvector**
- Supabase project (auth + `user_org` table)
- API keys: Groq and/or OpenAI, Cloudflare Workers AI embeddings, AWS S3 (optional), Razorpay (optional)

---

## Configuration

Set environment variables (or extend `app/core/config.py`). Common settings:

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | Async Postgres URL (`postgresql+asyncpg://…`) |
| `SPB_CONN_URL`, `SPB_KEY` | Supabase connection |
| `OPENAI_API_KEY`, `LLM_MODEL_NAME_OPENAI` | OpenAI chat models |
| `LLM_MODEL_NAME`, `LLM_MAX_TOKENS`, `LLM_TEMPERATURE` | Groq LLM settings |
| `CLOUDFLARE_EMBEDDING_MODEL_NAME` | Embedding model on Workers AI |
| `AWS_ACCESS_KEY`, `AWS_SECRET_KEY`, `AWS_REGION`, `S3_BUCKET_NAME` | File storage |
| `RAZORPAY_WEBHOOK_SECRET` | Payment webhooks |
| `MANAGED_BACKEND` | External managed inbox base URL |
| `VECTOR_STORE_TABLE_NAME` | pgvector table name (default: `pg_vector_store`) |

---

## Local Development

From the repository root:

```bash
cd app
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=$(pwd)/..
export DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/backend_agents

python -m uvicorn backend_app:app --reload --host 0.0.0.0 --port 8000
```

- **API docs:** http://localhost:8000/docs  
- **API prefix:** `/api/v1`

Tables are created automatically on startup via `wait_for_db` when the database is reachable.

---

## Docker

**Application stack** (API, database, reverse proxy):

```bash
docker compose up --build
```

**Datafeed workers** (embedding and URL scraping):

```bash
docker compose -f datafeed-docker-compose.yml up --build
```

---

## API Overview

All routes are under `/api/v1` unless noted. Authenticated routes expect `Authorization: Bearer <supabase_access_token>`.

| Area | Endpoints (examples) |
|------|----------------------|
| **Chatbots** | `GET /fetch_chatbots_details`, `POST /build_chatbot`, `POST /save_chatbot`, `GET /get_bot_details` |
| **Conversations** | `GET /get_conversations`, `GET /conversation` |
| **Messages** | `GET /message` (SSE stream), `GET /get_messages`, `POST /managed_conversation_message` |
| **Datafeeds** | `POST /text_data_upload`, `POST /upload_file`, `POST /scrape_url`, `GET /get_data_feed` |
| **Tags** | `GET /get_tag_data`, `POST /add_tag` |
| **Leads** | `POST /save_lead_form`, `POST /save_lead`, `GET /retrieve_leads` |
| **Tickets** | `POST /submit_chatbot_ticket`, `GET /get_tickets` |
| **Payments** | `POST /razorpay_webhook` |

Public chatbots can accept messages without a user token when `isPublic` is set; private bots require authentication.

---

## License

Proprietary — see repository owner for terms.
