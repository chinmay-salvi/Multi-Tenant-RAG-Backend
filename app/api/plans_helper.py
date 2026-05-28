import logging
from datetime import datetime
from aiocache import cached, SimpleMemoryCache
from dateutil import parser
from fastapi import HTTPException, status
from app.db.supabase import supabase_client


logger = logging.getLogger(__name__)


def fetch_plan_data(org_id: str) -> dict | None:
    """
    Directly queries the Supabase database to fetch subscription and trial limits for a tenant org.

    This function does the following:
    1. Checks if the organization has an active Razorpay subscription.
    2. If active Razorpay records exist, it parses their start/end dates and retrieves the limits from
       the associated row in the 'plans' table.
    3. If no active Razorpay subscription exists, it falls back to the organization's 'user_trial' record,
       parsing its trial start, end, and limits stored in a JSON configuration.

    Args:
        org_id (str): Unique string ID of the tenant organization.

    Returns:
        dict | None: A dictionary containing 'razorpay_subscriptions', 'user_trial', and 'plans' mappings.
                     Returns None if the database operation fails.
    """
    try:
        # Query active Razorpay subscriptions from Supabase
        razorpay_query = (
            supabase_client.table("razorpay_subscriptions")
            .select("status,current_start,current_end,plan_id")
            .eq("org_id", org_id)
            .eq("status", "active")
        )

        razorpay_data = razorpay_query.execute().data

        # If a subscription exists, parse datetimes and retrieve matching plan details
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

        # Query limits metadata if plan IDs exist
        if plan_ids:
            plans_query = (
                supabase_client.table("plans")
                .select("id, plan_notes")
                .in_("id", plan_ids)
            )
            plans_data = plans_query.execute().data

            user_trial_data = []
        else:
            # Query user_trial metadata as fallback if no Razorpay subscription is active
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


# Caches plan data in local memory for 120 seconds to reduce Supabase query loads
@cached(cache=SimpleMemoryCache, key=lambda *args, **kwargs: args[0], ttl=120)
async def fetch_cached_plan_data(org_id: str) -> dict | None:
    """
    A cached wrapper around fetch_plan_data to load-balance database accesses.

    Args:
        org_id (str): Organization tenant ID.

    Returns:
        dict | None: Mapped plan data dictionary.
    """
    return fetch_plan_data(org_id)


def check_datetime_validity(start_datetime: datetime, end_datetime: datetime) -> bool:
    """
    Asserts if the current machine time lies within a specified subscription or trial window.

    Ensures timezone consistency by converting current local time to the input timezone.

    Args:
        start_datetime (datetime): Window beginning.
        end_datetime (datetime): Window expiration.

    Returns:
        bool: True if the current time is inside the bounds; False otherwise.
    """
    # Get the current datetime ensuring same time zone constraints
    current_datetime = datetime.now(start_datetime.tzinfo)

    # Check if the current datetime is between the two datetimes
    return start_datetime <= current_datetime <= end_datetime


def check_datafeed_token_limit(org_id: str, utilised_token_count: int) -> str | None:
    """
    Validates if a tenant organization's total token count remains within their plan limits.

    It validates:
    1. Active subscription current window validity.
    2. Exceedance of maximum tokens allowed in plans/trial quotas.

    Args:
        org_id (str): Organization tenant ID.
        utilised_token_count (int): Sum of tokens across all active datafeeds for this tenant.

    Returns:
        str | None: An error message description if the limit is exceeded or plan expired;
                    None if the organization is active and compliant.
    """
    try:
        plan_data = fetch_plan_data(org_id)

        if not plan_data:
            return "No active plan found."

        razorpay_subscriptions = plan_data.get("razorpay_subscriptions", [])
        user_trial = plan_data.get("user_trial", [])
        plans = plan_data.get("plans", [])

        # Process Razorpay plan limits
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

        # Process standard trial limits
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
    """
    Validates if a tenant organization's active chatbot count remains within plan limits.

    Asserts limits stored under max_chatbots (Razorpay) or max_chatbot (Trial).

    Args:
        org_id (str): Organization tenant ID.
        utilised_chatbot_count (int): Sum of active chatbots deployed under this organization.

    Returns:
        str | None: An error message description if limits are exceeded;
                    None if compliant.
    """
    try:
        plan_data = fetch_plan_data(org_id)

        if not plan_data:
            return "No active plan found."

        razorpay_subscriptions = plan_data.get("razorpay_subscriptions", [])
        user_trial = plan_data.get("user_trial", [])
        plans = plan_data.get("plans", [])

        # Process Razorpay limits
        if razorpay_subscriptions:
            subscription = razorpay_subscriptions[0]
            if not check_datetime_validity(
                subscription["current_start"], subscription["current_end"]
            ):
                return "Plan period over, please resubscribe."
            if int(plans[0]["plan_notes"]["max_chatbots"]) <= utilised_chatbot_count:
                return "Max chatbot creation reached, please upgrade."

        # Process trial limits
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


async def get_limits(org_id: str) -> tuple[int, int]:
    """
    A helper function that returns the absolute limits (messages and tokens) active for an org.

    Acts as an entrypoint configuration validator for API routes.

    Args:
        org_id (str): Organization tenant ID.

    Returns:
        tuple[int, int]: (message_limit, token_limit) active for the organization.

    Raises:
        HTTPException: If plan data is missing or holds corrupt state.
    """
    # Fetch cached plan data
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
