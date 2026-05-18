from functools import lru_cache

from supabase import Client, create_client

from app.core.config import SPB_CONN_URL, SPB_KEY


@lru_cache
def get_supabase_client() -> Client:
    if not SPB_CONN_URL or not SPB_KEY:
        raise RuntimeError(
            "Supabase is not configured. Set SPB_CONN_URL and SPB_KEY in app/core/config.py."
        )
    return create_client(SPB_CONN_URL, SPB_KEY)


class _SupabaseClientProxy:
    """Lazy client so the app can boot before Supabase is configured."""

    def __getattr__(self, name: str):
        return getattr(get_supabase_client(), name)


supabase_client = _SupabaseClientProxy()
