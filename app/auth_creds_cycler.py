import asyncio
import datetime
import logging
import random
from typing import Dict

from app.api.crud_helper import fetch_random_auth_cred
from app.core.config import AUTH_CRED_REFRESH_MINUTES
from app.db.session import SessionLocal


logger = logging.getLogger(__name__)


lock = asyncio.Lock()
auth_creds_circular_arrays = dict()


class AuthCredsCircularArray:
    def __init__(self, auth_cred_type):
        self.data = []
        self.size = 0
        self.index = 0
        self.auth_cred_type = auth_cred_type
        self.refresh_time = datetime.datetime.min

    async def initialize(self):
        now = datetime.datetime.now()

        if now - self.refresh_time > datetime.timedelta(
            minutes=AUTH_CRED_REFRESH_MINUTES
        ):
            logger.info(
                f"Refreshing {self.auth_cred_type} AuthCredsCircularArray {now}"
            )

            try:
                async with SessionLocal() as session:
                    records = await fetch_random_auth_cred(
                        db=session, auth_cred_type=self.auth_cred_type
                    )
                    self.data = [eval(record) for record in records]

                random.shuffle(self.data)
                self.size = len(self.data)
                self.index = 0
                self.refresh_time = now
            except Exception as e:
                logger.error(f"Failed to refresh auth creds: {e}")
                self.data = []
                self.size = 0

    def get_next(self):
        if self.size == 0:
            return None
        item = self.data[self.index]
        self.index = (self.index + 1) % self.size
        return item


async def get_auth_creds(auth_cred_type) -> Dict:
    global auth_creds_circular_arrays

    async with lock:
        if auth_cred_type not in auth_creds_circular_arrays:
            auth_creds_circular_arrays[auth_cred_type] = AuthCredsCircularArray(
                auth_cred_type
            )
        await auth_creds_circular_arrays[auth_cred_type].initialize()
        return auth_creds_circular_arrays[auth_cred_type].get_next()
