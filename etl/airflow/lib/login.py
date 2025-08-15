import json
import os
import api


def login(username: str, password: str, user_name: str = None):
    """Login to UDisc API and return the response data"""
    response = api.post(
        "/login",
        json={
            "username": username,
            "password": password
        }
    )

    if response.ok:
        print("Login succeeded.")
        return response.json()
    else:
        print(f"Login failed (status = {response.status_code})")
        print("Reason:", response.json()["error"])
        return None
