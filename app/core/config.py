SPB_CONN_URL: str = ""
SPB_KEY: str = ""

RAZORPAY_WEBHOOK_SECRET: str = ""
DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/backend_agents"
PROJECT_NAME: str = "backend-agents"
LOG_LEVEL: str = "INFO"
API_PREFIX: str = "/api"

AWS_ACCESS_KEY: str = ""
AWS_SECRET_KEY: str = ""
AWS_REGION: str = ""
S3_BUCKET_NAME: str = ""


VECTOR_STORE_TABLE_NAME: str = "pg_vector_store"


NODE_PARSER_CHUNK_SIZE: int = 512
NODE_PARSER_CHUNK_OVERLAP: int = 10

CLOUDFLARE_EMBEDDING_MODEL_NAME: str = ""
CLOUDFLARE_EMBEDDING_MODEL_DIMENSION: int = 768


LLM_MODEL_NAME: str = ""
LLM_MAX_TOKENS: int = 1024
LLM_TEMPERATURE: float = 0.5
OPENAI_API_KEY: str = ""
LLM_MODEL_NAME_OPENAI: str = ""

WORKERS: int = 1

AUTH_CRED_REFRESH_MINUTES: int = 30

MANAGED_BACKEND: str = ""
