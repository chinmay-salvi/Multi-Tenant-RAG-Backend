import requests
from app.core.config import MANAGED_BACKEND


def notify_managed_backend(account, conversation, message, bot_token):
    data = {"content": message}
    url = f"{MANAGED_BACKEND}/api/v1/accounts/{account}/conversations/{conversation}/messages"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "api_access_token": f"{bot_token}",
    }
    r = requests.post(url, json=data, headers=headers)
    return r.json()


def change_conversation_status(account, conversation, bot_token):
    data = {"status": "open"}
    url = f"{MANAGED_BACKEND}/api/v1/accounts/{account}/conversations/{conversation}/toggle_status"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "api_access_token": f"{bot_token}",
    }
    r = requests.post(url, json=data, headers=headers)
    return r.json()
