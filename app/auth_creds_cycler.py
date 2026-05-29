import asyncio
import datetime
import logging
import random
from typing import Dict
import json
from app.user_org.crud import fetch_random_auth_cred
from app.core.config import AUTH_CRED_REFRESH_MINUTES
from app.db.session import SessionLocal


logger = logging.getLogger(__name__)


# Global asyncio lock to prevent race conditions during credential cyclic updates
lock = asyncio.Lock()

# Global registry holding cyclic structures mapping cred_type -> AuthCredsCircularArray
auth_creds_circular_arrays = dict()


class AuthCredsCircularArray:
    """
    A thread-safe circular array structure representing rotated third-party API credentials.

    It caches active system authentication records fetched from the database, shuffles them
    randomly to balance loads, and cycles through them sequentially on request. It automatically
    triggers a background reload from the database if the cache age exceeds the expiration limit
    defined by AUTH_CRED_REFRESH_MINUTES.
    """

    def __init__(self, auth_cred_type: str):
        """
        Initializes the cyclic circular array container.

        Args:
            auth_cred_type (str): The unique string identifier for the target API service
                                  (e.g., 'GROQ_LLM', 'CLOUDFLARE_EMBED').
        """
        self.data = []
        self.size = 0
        self.index = 0
        self.auth_cred_type = auth_cred_type
        self.refresh_time = datetime.datetime.min

    async def initialize(self):
        """
        Polls the database to populate the cyclic credentials list if cached data is stale.
        """
        now = datetime.datetime.now()

        # Check if the cache is older than the configured refresh duration
        if now - self.refresh_time > datetime.timedelta(
            minutes=AUTH_CRED_REFRESH_MINUTES
        ):
            logger.info(
                f"Refreshing {self.auth_cred_type} AuthCredsCircularArray {now}"
            )

            try:
                async with SessionLocal() as session:
                    # Query system credentials from SysAuthCred table
                    records = await fetch_random_auth_cred(
                        db=session, auth_cred_type=self.auth_cred_type
                    )
                    # Convert raw string rows (eval-safe serialized JSON dictionary strings) to Python dictionaries
                    self.data = [json.loads(record) for record in records]

                # Randomize order to load-balance across API keys
                random.shuffle(self.data)
                self.size = len(self.data)
                self.index = 0
                self.refresh_time = now
            except Exception as e:
                logger.error(f"Failed to refresh auth creds: {e}")
                self.data = []
                self.size = 0

    def get_next(self) -> Dict | None:
        """
        Retrieves the next API credential dictionary in sequence.

        Returns:
            Dict | None: The active API credentials configuration dict, or None if no keys exist.
        """
        if self.size == 0:
            return None
        item = self.data[self.index]
        # Circular indexing: increment and loop back if size limit reached
        self.index = (self.index + 1) % self.size
        return item


async def get_auth_creds(auth_cred_type: str) -> Dict | None:
    """
    Acts as the entrypoint function to fetch rotated API keys dynamically.

    Uses a global registry and circular structure to load-balance API requests,
    guaranteeing that database reloads occur in a single-threaded manner using a lock.

    Args:
        auth_cred_type (str): Unique identifier for the credential type (e.g. "GROQ_LLM").

    Returns:
        Dict | None: Next active credential set mapped as parsed dictionaries.
    """
    global auth_creds_circular_arrays

    async with lock:
        # Check if cyclic structure exists in registry, instantiate if missing
        if auth_cred_type not in auth_creds_circular_arrays:
            auth_creds_circular_arrays[auth_cred_type] = AuthCredsCircularArray(
                auth_cred_type
            )
        # Attempt to refresh / retrieve cyclic record
        await auth_creds_circular_arrays[auth_cred_type].initialize()
        return auth_creds_circular_arrays[auth_cred_type].get_next()
