import hashlib
import hmac
import json
from datetime import datetime

from fastapi import APIRouter
from fastapi import Request, HTTPException
from pydantic import BaseModel
from app.core.config import RAZORPAY_WEBHOOK_SECRET
from app.db.supabase import supabase_client


payments_router = APIRouter()


# Verify webhook signature
def verify_signature(payload, signature, secret):
    generated_signature = hmac.new(
        secret.encode(), payload.encode(), hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(generated_signature, signature)


# Define the request model
class RazorpayWebhook(BaseModel):
    event: str
    payload: dict


# Define the relevant Stripe events
relevant_events = {
    "product.created",
    "product.updated",
    "product.deleted",
    "price.created",
    "price.updated",
    "price.deleted",
    "checkout.session.completed",
    "customer.subscription.created",
    "customer.subscription.updated",
    "customer.subscription.deleted",
}


@payments_router.post("/razorpay_webhook")
async def webhook(request: Request):
    body = await request.body()
    signature = request.headers.get("X-Razorpay-Signature")

    if not signature:
        raise HTTPException(
            status_code=400, detail="X-Razorpay-Signature header missing"
        )

    if not verify_signature(body.decode("utf-8"), signature, RAZORPAY_WEBHOOK_SECRET):
        raise HTTPException(status_code=400, detail="Invalid signature")

    payload = json.loads(body)
    event = payload.get("event")

    # Handle subscription events
    if event.startswith("subscription."):
        await handle_subscription_event(event, payload)
    else:
        print("Event without Subscription:", payload)

    return {"status": "success"}


async def handle_subscription_event(event, payload):
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


def to_datetime(secs: int):
    return datetime.utcfromtimestamp(secs).isoformat() + "Z" if secs else None


async def handle_subscription_activated_event(event, payload):
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


async def handle_subscription_update_event(event, payload):
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
        # response = supabase.table('razorpay_subscriptions').update(update_data) \
        #     .eq('id', subscription.get("id")) \
        #     .execute()
    except Exception as e:
        print("error")
        print(e)
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred in Supabase in handle_subscription_update_event: {str(e)}",
        )

    print(f"Subscription {subscription.get('id')} updated successfully")


async def handle_subscription_update_main_event(event, payload):
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


async def handle_subscription_charged_event(event, payload):
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
