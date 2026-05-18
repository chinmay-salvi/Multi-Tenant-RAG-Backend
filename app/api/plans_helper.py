import logging
from datetime import datetime
from aiocache import cached, SimpleMemoryCache
from dateutil import parser
from fastapi import HTTPException, status
from app.db.supabase import supabase_client


logger = logging.getLogger(__name__)


def fetch_plan_data(org_id: str):
    try:
        razorpay_query = (
            supabase_client.table("razorpay_subscriptions")
            .select("status,current_start,current_end,plan_id")
            .eq("org_id", org_id)
            .eq("status", "active")
        )

        razorpay_data = razorpay_query.execute().data

        # Extract plan_ids from the razorpay_subscriptions data
        if razorpay_data:
            plan_ids = [item["plan_id"] for item in razorpay_data]
            razorpay_data[0]["current_start"] = parser.parse(
                razorpay_data[0]["current_start"]
            )
            razorpay_data[0]["current_end"] = parser.parse(
                razorpay_data[0]["current_end"]
            )
        else:
            plan_ids = []

        # Query plans for plan_notes using plan_ids from razorpay_subscriptions
        if plan_ids:
            plans_query = (
                supabase_client.table("plans")
                .select("id, plan_notes")
                .in_("id", plan_ids)
            )
            plans_data = plans_query.execute().data

            user_trial_data = []
        else:
            # Query user_trial for trial_start, trial_end, and notes
            user_trial_query = (
                supabase_client.table("user_trial")
                .select("trial_start, trial_end, notes")
                .eq("org_id", org_id)
            )
            user_trial_data = user_trial_query.execute().data

            if user_trial_data:
                user_trial_data[0]["trial_start"] = parser.parse(
                    user_trial_data[0]["trial_start"]
                )
                user_trial_data[0]["trial_end"] = parser.parse(
                    user_trial_data[0]["trial_end"]
                )

            plans_data = []
    except Exception as e:
        print("Error occurred while fetching plan", e)
        return None

    return {
        "razorpay_subscriptions": razorpay_data,
        "user_trial": user_trial_data,
        "plans": plans_data,
    }


@cached(cache=SimpleMemoryCache, key=lambda *args, **kwargs: args[0], ttl=120)
async def fetch_cached_plan_data(org_id: str):
    return fetch_plan_data(org_id)


def check_datetime_validity(start_datetime: datetime, end_datetime: datetime) -> bool:
    # Get the current datetime ensuring same time
    current_datetime = datetime.now(start_datetime.tzinfo)

    # Check if the current datetime is between the two datetimes
    return start_datetime <= current_datetime <= end_datetime


def check_datafeed_token_limit(org_id, utilised_token_count):
    try:
        plan_data = fetch_plan_data(org_id)

        if not plan_data:
            return "No active plan found."

        razorpay_subscriptions = plan_data.get("razorpay_subscriptions", [])
        user_trial = plan_data.get("user_trial", [])
        plans = plan_data.get("plans", [])

        if razorpay_subscriptions:
            subscription = razorpay_subscriptions[0]
            if not check_datetime_validity(
                subscription["current_start"], subscription["current_end"]
            ):
                return "Plan period over, please resubscribe."
            if (
                int(plans[0]["plan_notes"]["max_datafeed_tokens"])
                <= utilised_token_count
            ):
                return "Max tokens reached, please upgrade."

        elif user_trial:
            trial = user_trial[0]
            if not check_datetime_validity(trial["trial_start"], trial["trial_end"]):
                return "Plan period over, please subscribe."

            if int(trial["notes"]["max_datafeed_tokens"]) <= utilised_token_count:
                return "Max tokens reached, please upgrade."

        else:
            return "No active plan found."

    except Exception as e:
        print(f"An error occurred: {e}")
        return "Unexpected error has occurred in plan."

    return None


def check_chatbot_limit(org_id: str, utilised_chatbot_count: int) -> str | None:
    try:
        plan_data = fetch_plan_data(org_id)

        if not plan_data:
            return "No active plan found."

        razorpay_subscriptions = plan_data.get("razorpay_subscriptions", [])
        user_trial = plan_data.get("user_trial", [])
        plans = plan_data.get("plans", [])

        if razorpay_subscriptions:
            subscription = razorpay_subscriptions[0]
            if not check_datetime_validity(
                subscription["current_start"], subscription["current_end"]
            ):
                return "Plan period over, please resubscribe."
            if int(plans[0]["plan_notes"]["max_chatbots"]) <= utilised_chatbot_count:
                return "Max chatbot creation reached, please upgrade."

        elif user_trial:
            trial = user_trial[0]
            if not check_datetime_validity(trial["trial_start"], trial["trial_end"]):
                return "Plan period over, please subscribe."
            if int(trial["notes"]["max_chatbot"]) <= utilised_chatbot_count:
                return "Max chatbot creation reached, please upgrade."

        else:
            return "No active plan found."

    except Exception as e:
        print(f"An error occurred: {e}")
        return "Unexpected error has occurred in plan."

    return None


async def get_limits(org_id: str):
    # Fetch plan data
    plan_data = await fetch_cached_plan_data(org_id)

    if not plan_data:
        logger.error("Error occurred while fetching plan data.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error occurred while fetching plan data.",
        )

    # Determine token limit based on the plan data
    razorpay_subscriptions = plan_data.get("razorpay_subscriptions", [])
    user_trial = plan_data.get("user_trial", [])
    plans = plan_data.get("plans", [])

    if razorpay_subscriptions:
        token_limit = int(plans[0]["plan_notes"]["max_datafeed_tokens"])
        message_limit = int(plans[0]["plan_notes"]["max_messages"])
    elif user_trial:
        token_limit = int(user_trial[0]["notes"]["max_datafeed_tokens"])
        message_limit = int(user_trial[0]["notes"]["max_message"])
    else:
        logger.error("Unexpected state reached while checking plan data.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected state reached while checking plan data.",
        )

    return message_limit, token_limit
