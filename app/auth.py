from fastapi import Header, HTTPException, Request
from app.db.supabase import supabase_client


async def validate_user(request: Request, authorization: str = Header(...)) -> dict:
    """
    Validates the Supabase JWT from the Authorization header and resolves multi-tenant org constraints.

    This function acts as a FastAPI dependency for authenticated endpoints. It performs the following checks:
    1. Extracts and format-validates the Bearer token from the 'Authorization' header.
    2. Retrieves the corresponding user info from Supabase Auth (`supabase_client.auth.get_user`).
    3. Resolves the user's organization (`orgId`) and role mapping from the `user_org` database table.
    4. Validates that the `userId` in the payload matches the `userId` present in the request's query, path, or body.

    Args:
        request (Request): The incoming FastAPI Request object used to parse URL params/path params or body values.
        authorization (str): The HTTP Authorization header containing the Bearer token.

    Returns:
        dict: A payload dictionary with the following schema:
            {
                "userId": str(UUID),
                "orgId": str(UUID),
                "userRole": str,
                "userCreateAt": datetime
            }

    Raises:
        HTTPException:
            - 400 Bad Request: If token format is invalid, missing, or user ID cannot be found in request.
            - 401 Unauthorized: If the token is invalid or request user ID does not match the token owner.
            - 404 Not Found: If the user does not exist in Supabase auth.
            - 405 Method Not Allowed: If request uses an unsupported HTTP verb.
            - 500 Internal Server Error: If DB query for roles/orgs fails.
    """

    # Check if the header is formatted correctly as a Bearer token
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=400, detail="Invalid token format")

    try:
        # Extract the token part by removing the "Bearer " prefix
        access_token = authorization[len("Bearer ") :]
    except (AttributeError, IndexError):
        raise HTTPException(status_code=400, detail="No authentication token provided")

    try:
        # Attempt to fetch the authenticated user profile using Supabase
        response = supabase_client.auth.get_user(access_token)
        supabase_user_id = response.user.id
        payload = {"userId": supabase_user_id, "userCreateAt": response.user.created_at}
    except Exception as e:
        print(f"An error occurred while authenticating user: {e}")
        raise HTTPException(status_code=404, detail="User not found")

    try:
        # Resolve the organization association and role from Supabase 'user_org' table
        response = (
            supabase_client.table("user_org")
            .select("*")
            .eq("user_id", supabase_user_id)
            .execute()
        )

        # If organizational association is found, map it to payload
        if response and response.data:
            first_record = response.data[0]
            user_id = first_record["user_id"]
            org_id = first_record["org_id"]
            role = first_record["role"]

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

    # Multi-tenant safety check: Validate request user matches token user
    if request.method == "GET":
        # Extract user_id from URL parameters or path variables
        user_id = request.query_params.get("userId") or request.path_params.get(
            "userId"
        )
    elif request.method == "POST":
        # Specific routes generate payload values directly on backend, bypassing body matches
        if request.url.path in {
            "/api/v1/upload_file",
            "/api/v1/save_chatbot",
            "/api/v1/submit_chatbot_ticket",
        }:
            user_id = payload["userId"]
        else:
            # Parse the request JSON body to assert identity
            data = await request.json()
            user_id = data.get("userId")
    else:
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if user_id is None:
        raise HTTPException(status_code=400, detail="User Id not found in the request.")

    # Prevent cross-user access: Token owner must be the same as target user ID
    if payload["userId"] != user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    return payload
