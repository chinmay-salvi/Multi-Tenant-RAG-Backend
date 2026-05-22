# Backend Agents

Backend service for a no-code AI chatbot platform. Organizations build configurable chatbots grounded in custom knowledge (datafeeds), deploy them via widgets or managed channels, and handle conversations, leads, support tickets, and billing through a unified FastAPI API.

## Features

- **Multi-tenant chatbots** — Organizations, users, tags, and per-chatbot configuration (purpose, company context, intro messages, branding).
- **Retrieval-augmented chat (RAG)** — Answers are grounded in embedded datafeeds stored in PostgreSQL with **pgvector**.
- **Streaming responses** — Real-time assistant replies over **Server-Sent Events (SSE)** with subprocess metadata for transparency.
- **Conversation memory** — Full message history persisted per conversation; recent turns plus **query condensation** keep follow-up questions coherent in longer threads.
- **Knowledge ingestion** — Upload text/files, scrape URLs, and background workers for embedding and scraping pipelines.
- **Managed backend integration** — Webhook-style messaging for external inbox platforms with optional human handoff.
- **Leads & support** — Capture lead forms, ticket submission, and status workflows.
- **Plans & payments** — Trial and subscription limits with **Razorpay** webhook handling.

## Architecture

```
┌─────────────┐     ┌──────────────────────────────────────────────────┐
│   Client    │────▶│  FastAPI (nginx)  /api/v1/*                      │
│  (widget)   │     │  • Auth (Supabase JWT)                           │
└─────────────┘     │  • Chatbots, conversations, messages (SSE)       │
                    │  • Datafeeds, leads, tickets, payments          │
                    └───────────┬──────────────────────┬───────────────┘
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

## Project structure

```
backend-agents-main/
├── app/                          # Main FastAPI application
│   ├── api/endpoints/            # REST routers (chatbot, message, datafeed, …)
│   ├── chat/                     # RAG, agents, streaming callbacks
│   ├── core/config.py            # Environment configuration
│   ├── db/                       # SQLAlchemy models, sessions, pgvector
│   ├── backend_app.py            # App entrypoint
│   └── requirements.txt
├── datafeed_processing/          # Background embed & scrape workers
├── docker-compose.yml            # App + Postgres (pgvector) + nginx
├── datafeed-docker-compose.yml   # Embed and scrape worker services
└── commands.txt                  # Local dev quick reference
```

## Tech stack

| Layer | Technology |
|--------|------------|
| API | FastAPI, Uvicorn, SSE (sse-starlette) |
| ORM / DB | SQLAlchemy (async), PostgreSQL, pgvector |
| RAG | LlamaIndex (CondensePlusContextChatEngine, VectorStoreIndex) |
| Auth | Supabase |
| Storage | AWS S3 |
| Payments | Razorpay webhooks |
| Workers | Dockerized embed/scrape services |

## Prerequisites

- Python 3.11+
- PostgreSQL 16 with **pgvector**
- Supabase project (auth + `user_org` table)
- API keys: Groq and/or OpenAI, Cloudflare Workers AI embeddings, AWS S3 (optional), Razorpay (optional)

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

## Local development

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

## Docker

**Application stack** (API, database, reverse proxy):

```bash
docker compose up --build
```

**Datafeed workers** (embedding and URL scraping):

```bash
docker compose -f datafeed-docker-compose.yml up --build
```

## API overview

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

## Datafeeds

Knowledge sources are attached to chatbots and embedded into the shared vector index:

- **Text** — Direct paste/upload.
- **Files** — PDF, DOCX, JSON, TXT (stored in S3, processed by the embed worker).
- **URLs** — Submitted for scraping; selected pages are parsed and embedded.

Chunks use a configurable size (`NODE_PARSER_CHUNK_SIZE`, default 512) and overlap. Retrieval applies metadata filters so each chatbot only sees its org’s relevant nodes.

## Conversations & memory

- Every turn is stored as a `Message` with role, content, status, and optional **sub-processes** (e.g. query engine construction).
- For generation, recent conversation turns are passed into the chat engine together with **condensation** so short follow-ups (“what about pricing?”) become self-contained questions.
- Intro messages are seeded automatically on the first user message in a new conversation.

## Plans & usage

Organizations are associated with trial or Razorpay subscription plans. Usage checks cover chatbot counts, message volume, and datafeed token utilization against plan limits stored in Supabase-cached plan data.

## License

Proprietary — see repository owner for terms.
