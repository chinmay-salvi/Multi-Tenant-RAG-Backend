from datetime import datetime
from dateutil import parser
from supabase import create_client

from config import SPB_CONN_URL, SPB_KEY


supabase_client = create_client(SPB_CONN_URL, SPB_KEY) if SPB_CONN_URL and SPB_KEY else None


def fetch_plan_data(org_id: str) -> dict | None:
    """
    Directly queries the Supabase database to fetch subscription and trial limits for a background worker org.

    This background script version of fetch_plan_data mirrors the API gateway logic, utilizing
    the standard Supabase client directly without async dependencies (as workers run synchronously or
    via standard threads).

    Args:
        org_id (str): Unique string ID of the tenant organization.

    Returns:
        dict | None: Mapped plan, Razorpay, or user trial limits dictionary.
    """
    if supabase_client is None:
        return None
    try:
        # Query active subscription tables in Supabase
        razorpay_query = (
            supabase_client.table("razorpay_subscriptions")
            .select("status,current_start,current_end,plan_id")
            .eq("org_id", org_id)
            .eq("status", "active")
        )

        razorpay_data = razorpay_query.execute().data

        # If a subscription exists, parse datetimes and retrieve matching plan details
        plan_ids = [item["plan_id"] for item in razorpay_data] if razorpay_data else []

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
            plans_data = []
    except Exception as e:
        print("Error occurred while fetching plan", e)
        return None

    return {
        "razorpay_subscriptions": razorpay_data,
        "user_trial": user_trial_data,
        "plans": plans_data,
    }


def check_datetime_validity(start_datetime_str: str, end_datetime_str: str) -> bool:
    """
    Asserts if the current machine time lies within a specified subscription or trial window.

    Ensures timezone consistency by converting current local time to the input timezone.

    Args:
        start_datetime_str (str): ISO formatted start string.
        end_datetime_str (str): ISO formatted end string.

    Returns:
        bool: True if the current time is inside the bounds; False otherwise.
    """
    # Parse the string datetimes into datetime objects
    start_datetime = parser.parse(start_datetime_str)
    end_datetime = parser.parse(end_datetime_str)

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
                return "Plan period over, please resubscribe."

            if int(trial["notes"]["max_datafeed_tokens"]) <= utilised_token_count:
                return "Max tokens reached, please upgrade."

        else:
            return "No active plan found."

    except Exception as e:
        print(f"An error occurred: {e}")
        return "Unexpected error has occurred in plan."

    return None
