from datetime import datetime
from dateutil import parser
from supabase import create_client

from config import SPB_CONN_URL, SPB_KEY


supabase_client = create_client(SPB_CONN_URL, SPB_KEY) if SPB_CONN_URL and SPB_KEY else None


def fetch_plan_data(org_id: str):
    if supabase_client is None:
        return None
    try:
        razorpay_query = (
            supabase_client.table("razorpay_subscriptions")
            .select("status,current_start,current_end,plan_id")
            .eq("org_id", org_id)
            .eq("status", "active")
        )

        razorpay_data = razorpay_query.execute().data

        # Extract plan_ids from the razorpay_subscriptions data
        plan_ids = [item["plan_id"] for item in razorpay_data] if razorpay_data else []

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
    # Parse the string datetimes into datetime objects
    start_datetime = parser.parse(start_datetime_str)
    end_datetime = parser.parse(end_datetime_str)

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
                return "Plan period over, please resubscribe."

            if int(trial["notes"]["max_datafeed_tokens"]) <= utilised_token_count:
                return "Max tokens reached, please upgrade."

        else:
            return "No active plan found."

    except Exception as e:
        print(f"An error occurred: {e}")
        return "Unexpected error has occurred in plan."

    return None
