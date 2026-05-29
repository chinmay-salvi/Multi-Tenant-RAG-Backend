import hashlib
import hmac
import json
import logging
from datetime import datetime

from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
from app.core.config import RAZORPAY_WEBHOOK_SECRET
from app.db.supabase import supabase_client

logger = logging.getLogger(__name__)

payments_router = APIRouter()


# Verify webhook signature
def verify_signature(payload: str, signature: str, secret: str) -> bool:
    """
    Verifies the authenticity of incoming Razorpay webhook signatures using HMAC-SHA256.

    Args:
        payload (str): The raw text request body bytes string decoded as UTF-8.
        signature (str): The signature provided in the 'X-Razorpay-Signature' header.
        secret (str): Webhook verification key.

    Returns:
        bool: True if signature is authentic and matches; False otherwise.
    """
    generated_signature = hmac.new(
        secret.encode(), payload.encode(), hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(generated_signature, signature)


# Define the request model
class RazorpayWebhook(BaseModel):
    event: str
    payload: dict


@payments_router.post("/razorpay_webhook")
async def webhook(request: Request) -> dict:
    """
    Acts as the target API receiver for Razorpay subscription status updates.

    Asserts signature authenticity before routing the event parameters to matching subscription
    state event handler functions.

    Args:
        request (Request): Webhook HTTP Request object holding payloads and signature headers.

    Returns:
        dict: A success acknowledgement JSON response.

    Raises:
        HTTPException:
            - 400 Bad Request: If signature header is missing or signature verification fails.
    """
    body = await request.body()
    signature = request.headers.get("X-Razorpay-Signature")

    # Guard against unsigned webhook events
    if not signature:
        raise HTTPException(
            status_code=400, detail="X-Razorpay-Signature header missing"
        )

    # Prevent malicious event spoofing
    if not verify_signature(body.decode("utf-8"), signature, RAZORPAY_WEBHOOK_SECRET):
        raise HTTPException(status_code=400, detail="Invalid signature")

    payload = json.loads(body)
    event = payload.get("event")

    # Route subscription event types specifically
    if event.startswith("subscription."):
        await handle_subscription_event(event, payload)
    else:
        print("Event without Subscription:", payload)

    return {"status": "success"}


async def handle_subscription_event(event: str, payload: dict) -> None:
    """
    Routes Razorpay subscription events to target database synchronization handlers.

    Args:
        event (str): Webhook event name (e.g. 'subscription.activated').
        payload (dict): JSON payload values containing entity states.
    """
    if event == "subscription.authenticated":
        # Handle subscription authenticated event
        print("Subscription authenticated:", payload)
    elif event == "subscription.charged":
        # Handle subscription charged event
        print("Subscription charged:", payload)
        await handle_subscription_charged_event(event, payload)
    elif event == "subscription.activated":
        # Handle subscription charged event
        print("Subscription activated:", payload)
        await handle_subscription_activated_event(event, payload)
    elif event == "subscription.completed":
        # Handle subscription completed event
        print("Subscription completed:", payload)
        await handle_subscription_update_event(event, payload)
    elif event == "subscription.paused":
        # Handle subscription paused event
        print("Subscription paused:", payload)
        await handle_subscription_update_event(event, payload)
    elif event == "subscription.resumed":
        # Handle subscription resumed event
        print("Subscription resumed:", payload)
        await handle_subscription_update_event(event, payload)
    elif event == "subscription.halted":
        # Handle subscription halted event
        print("Subscription halted:", payload)
        await handle_subscription_update_event(event, payload)
    elif event == "subscription.cancelled":
        # Handle subscription cancelled event
        print("Subscription cancelled:", payload)
        await handle_subscription_update_event(event, payload)
    elif event == "subscription.updated":
        # Handle subscription updated event
        print("Subscription updated:", payload)
        await handle_subscription_update_main_event(event, payload)
    elif event == "subscription.pending":
        # Handle subscription updated event
        print("Subscription pending:", payload)
        await handle_subscription_update_event(event, payload)
    else:
        print(f"Unhandled event type: {event}")


def to_datetime(secs: int) -> str | None:
    """
    Converts epoch seconds integers to ISO formatted UTC timestamp strings.

    Args:
        secs (int): Epoch seconds parameter.

    Returns:
        str | None: ISO timestamp string (e.g. '2024-01-01T00:00:00Z'), or None if secs is missing.
    """
    return datetime.utcfromtimestamp(secs).isoformat() + "Z" if secs else None


async def handle_subscription_activated_event(event: str, payload: dict) -> None:
    """
    Syncs Supabase 'razorpay_subscriptions' columns when a subscription is activated.

    Args:
        event (str): Webhook event status descriptor.
        payload (dict): Webhook body containing subscription updates.

    Raises:
        HTTPException: If Supabase connection fails.
    """
    subscription = payload["payload"]["subscription"]["entity"]
    update_data = {
        "plan_id": subscription.get("plan_id"),
        "razorpay_customer_id": subscription.get("customer_id"),
        "status": subscription.get("status"),
        "notes": subscription.get("notes"),
        "quantity": subscription.get("quantity"),
        "current_start": to_datetime(subscription.get("current_start")),
        "current_end": to_datetime(subscription.get("current_end")),
        "ended_at": to_datetime(subscription.get("ended_at")),
        "charge_at": to_datetime(subscription.get("charge_at")),
        "start_at": to_datetime(subscription.get("start_at")),
        "end_at": to_datetime(subscription.get("end_at")),
        "auth_attempts": subscription.get("auth_attempts"),
        "total_count": subscription.get("total_count"),
        "paid_count": subscription.get("paid_count"),
        "customer_notify": subscription.get("customer_notify"),
        "created_at": to_datetime(subscription.get("created_at")),
        "expire_by": to_datetime(subscription.get("expire_by")),
        "short_url": subscription.get("short_url"),
        "has_scheduled_changes": subscription.get("has_scheduled_changes"),
        "change_scheduled_at": to_datetime(subscription.get("change_scheduled_at")),
        "source": subscription.get("source"),
        "offer_id": subscription.get("offer_id"),
        "remaining_count": subscription.get("remaining_count"),
    }

    try:
        response = (
            supabase_client.table("razorpay_subscriptions")
            .update(update_data)
            .eq("id", subscription.get("id"))
            .eq("plan_id", subscription.get("plan_id"))
            .execute()
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred in Supabase in handle_subscription_activated_event: {str(e)}",
        )

    print(f"Subscription {subscription.get('id')} updated successfully")


async def handle_subscription_update_event(event: str, payload: dict) -> None:
    """
    Syncs Supabase status fields when standard updates (paused, completed, cancelled) occur.

    Filters updates matching composite keys (id + plan_id).

    Args:
        event (str): Webhook event status descriptor.
        payload (dict): Webhook body containing subscription updates.

    Raises:
        HTTPException: If Supabase connection fails.
    """
    subscription = payload["payload"]["subscription"]["entity"]
    update_data = {
        "plan_id": subscription.get("plan_id"),
        "razorpay_customer_id": subscription.get("customer_id"),
        "status": subscription.get("status"),
        "notes": subscription.get("notes"),
        "quantity": subscription.get("quantity"),
        "current_start": to_datetime(subscription.get("current_start")),
        "current_end": to_datetime(subscription.get("current_end")),
        "ended_at": to_datetime(subscription.get("ended_at")),
        "charge_at": to_datetime(subscription.get("charge_at")),
        "start_at": to_datetime(subscription.get("start_at")),
        "end_at": to_datetime(subscription.get("end_at")),
        "auth_attempts": subscription.get("auth_attempts"),
        "total_count": subscription.get("total_count"),
        "paid_count": subscription.get("paid_count"),
        "customer_notify": subscription.get("customer_notify"),
        "created_at": to_datetime(subscription.get("created_at")),
        "expire_by": to_datetime(subscription.get("expire_by")),
        "short_url": subscription.get("short_url"),
        "has_scheduled_changes": subscription.get("has_scheduled_changes"),
        "change_scheduled_at": to_datetime(subscription.get("change_scheduled_at")),
        "source": subscription.get("source"),
        "offer_id": subscription.get("offer_id"),
        "remaining_count": subscription.get("remaining_count"),
    }

    try:
        response = (
            supabase_client.table("razorpay_subscriptions")
            .update(update_data)
            .eq("id", subscription.get("id"))
            .eq("plan_id", subscription.get("plan_id"))
            .execute()
        )
    except Exception as e:
        print("error")
        print(e)
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred in Supabase in handle_subscription_update_event: {str(e)}",
        )

    print(f"Subscription {subscription.get('id')} updated successfully")


async def handle_subscription_update_main_event(event: str, payload: dict) -> None:
    """
    Syncs Supabase status fields specifically during primary plan changes.

    Unlike standard update handlers, it asserts updates across the main identifier column (`id`) only.

    Args:
        event (str): Webhook event status descriptor.
        payload (dict): Webhook body containing subscription updates.

    Raises:
        HTTPException: If Supabase connection fails.
    """
    subscription = payload["payload"]["subscription"]["entity"]
    update_data = {
        "plan_id": subscription.get("plan_id"),
        "razorpay_customer_id": subscription.get("customer_id"),
        "status": subscription.get("status"),
        "notes": subscription.get("notes"),
        "quantity": subscription.get("quantity"),
        "current_start": to_datetime(subscription.get("current_start")),
        "current_end": to_datetime(subscription.get("current_end")),
        "ended_at": to_datetime(subscription.get("ended_at")),
        "charge_at": to_datetime(subscription.get("charge_at")),
        "start_at": to_datetime(subscription.get("start_at")),
        "end_at": to_datetime(subscription.get("end_at")),
        "auth_attempts": subscription.get("auth_attempts"),
        "total_count": subscription.get("total_count"),
        "paid_count": subscription.get("paid_count"),
        "customer_notify": subscription.get("customer_notify"),
        "created_at": to_datetime(subscription.get("created_at")),
        "expire_by": to_datetime(subscription.get("expire_by")),
        "short_url": subscription.get("short_url"),
        "has_scheduled_changes": subscription.get("has_scheduled_changes"),
        "change_scheduled_at": to_datetime(subscription.get("change_scheduled_at")),
        "source": subscription.get("source"),
        "offer_id": subscription.get("offer_id"),
        "remaining_count": subscription.get("remaining_count"),
    }

    try:
        response = (
            supabase_client.table("razorpay_subscriptions")
            .update(update_data)
            .eq("id", subscription.get("id"))
            .execute()
        )
    except Exception as e:
        print("error")
        print(e)
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred in Supabase in handle_subscription_update_main_event: {str(e)}",
        )

    print(f"Subscription {subscription.get('id')} updated successfully")


async def handle_subscription_charged_event(event: str, payload: dict) -> None:
    """
    Syncs Supabase status fields and stores the payment identifier when a subscription transaction succeeds.

    Args:
        event (str): Webhook event status descriptor.
        payload (dict): Webhook body containing subscription charged updates.

    Raises:
        HTTPException: If Supabase connection fails.
    """
    subscription = payload["payload"]["subscription"]["entity"]
    payment = payload["payload"]["payment"]["entity"]
    update_data = {
        "plan_id": subscription.get("plan_id"),
        "razorpay_customer_id": subscription.get("customer_id"),
        "razorpay_payment_id": payment.get("id"),
        "status": subscription.get("status"),
        "notes": subscription.get("notes"),
        "quantity": subscription.get("quantity"),
        "current_start": to_datetime(subscription.get("current_start")),
        "current_end": to_datetime(subscription.get("current_end")),
        "ended_at": to_datetime(subscription.get("ended_at")),
        "charge_at": to_datetime(subscription.get("charge_at")),
        "start_at": to_datetime(subscription.get("start_at")),
        "end_at": to_datetime(subscription.get("end_at")),
        "auth_attempts": subscription.get("auth_attempts"),
        "total_count": subscription.get("total_count"),
        "paid_count": subscription.get("paid_count"),
        "customer_notify": subscription.get("customer_notify"),
        "created_at": to_datetime(subscription.get("created_at")),
        "expire_by": to_datetime(subscription.get("expire_by")),
        "short_url": subscription.get("short_url"),
        "has_scheduled_changes": subscription.get("has_scheduled_changes"),
        "change_scheduled_at": to_datetime(subscription.get("change_scheduled_at")),
        "source": subscription.get("source"),
        "offer_id": subscription.get("offer_id"),
        "remaining_count": subscription.get("remaining_count"),
    }
    try:
        response = (
            supabase_client.table("razorpay_subscriptions")
            .update(update_data)
            .eq("id", subscription.get("id"))
            .eq("plan_id", subscription.get("plan_id"))
            .execute()
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred in Supabase in handle_subscription_charged_event: {str(e)}",
        )

    print(f"Subscription {subscription.get('id')} updated successfully")
