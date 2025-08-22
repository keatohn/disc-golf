import requests

# iOS User Agent
IOS_USER_AGENT = "Mozilla/5.0 (iPhone; CPU iPhone OS 16_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Mobile/15E148 Safari/604.1"

# UDisc's Parse Application ID
PARSE_APP_ID = "X7O7gSaOUxCv9cTAHSASADcGtaRq7Kf9a4gNA8rn"

# Endpoint where UDisc's Parse server lives
PARSE_ENDPOINT = "https://udisc.xyz/parse"

# Base headers that ensure requests will work with the Parse server
BASE_HEADERS = {
    "User-Agent": IOS_USER_AGENT,
    "X-Parse-Application-Id": PARSE_APP_ID,
    "X-Parse-Revocable-Session": "1",
}


def get(endpoint: str, params=None, session_token=None):
    """Perform a GET request against the Parse server."""
    headers = BASE_HEADERS.copy()
    if session_token:
        headers["X-Parse-Session-Token"] = session_token

    session = requests.Session()
    return session.get(
        url=f"{PARSE_ENDPOINT}{endpoint}",
        headers=headers,
        params=params,
    )


def post(endpoint: str, json=None, session_token=None):
    """Perform a POST request against the parse server."""
    headers = BASE_HEADERS.copy()
    if session_token:
        headers["X-Parse-Session-Token"] = session_token

    session = requests.Session()
    return session.post(
        url=f"{PARSE_ENDPOINT}{endpoint}",
        headers=headers,
        json=json,
    )


def set_session(session_token: str):
    """Set the session token header to the session token. (Deprecated - use session_token parameter instead)"""
    print("Warning: set_session is deprecated. Pass session_token directly to get/post functions.")
    pass
