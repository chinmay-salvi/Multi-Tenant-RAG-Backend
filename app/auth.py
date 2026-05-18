from fastapi import Header, HTTPException, Request
from app.db.supabase import supabase_client


async def validate_user(request: Request, authorization: str = Header(...)):
    """
    :param request:
    :param authorization:
    :return:
    """

    # Check if the header is formatted correctly
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=400, detail="Invalid token format")

    try:
        # Extract the token part by removing "Bearer " prefix
        access_token = authorization[len("Bearer ") :]
    except (AttributeError, IndexError):
        raise HTTPException(status_code=400, detail="No authentication token provided")

    try:
        # Attempt to get the user with the provided access token
        response = supabase_client.auth.get_user(access_token)
        supabase_user_id = response.user.id
        payload = {"userId": supabase_user_id, "userCreateAt": response.user.created_at}
    except Exception as e:
        print(f"An error occurred while authenticating user: {e}")
        raise HTTPException(status_code=404, detail="User not found")

    try:
        response = (
            supabase_client.table("user_org")
            .select("*")
            .eq("user_id", supabase_user_id)
            .execute()
        )

        # Check if the response contains data
        if response and response.data:
            first_record = response.data[0]
            user_id = first_record["user_id"]
            org_id = first_record["org_id"]
            role = first_record["role"]

            # Print the values of the first row
            print(f"User ID: {user_id}", f"Org ID: {org_id}", f"Role: {role}")
            payload["orgId"] = org_id
            payload["userRole"] = role

    except Exception as e:
        print("Exception in auth:", e)
        raise HTTPException(
            status_code=500,
            detail="Internal server error while trying to find organization or role for the specified user.",
        )

    if payload is None:
        raise HTTPException(
            status_code=500,
            detail="Internal server error while trying to find organization or role for the specified user.",
        )

    if request.method == "GET":
        # Extract user_id from URL parameters
        user_id = request.query_params.get("userId") or request.path_params.get(
            "userId"
        )
    # Check if the request method is POST
    elif request.method == "POST":
        if request.url.path in {
            "/api/v1/upload_file",
            "/api/v1/save_chatbot",
            "/api/v1/submit_chatbot_ticket",
        }:
            user_id = payload["userId"]
        else:
            # Parse the request body
            data = await request.json()
            user_id = data.get("userId")
    else:
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if user_id is None:
        raise HTTPException(status_code=400, detail="User Id not found in the request.")

    if payload["userId"] != user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    return payload
