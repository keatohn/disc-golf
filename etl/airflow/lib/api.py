import requests

session: requests.Session = requests.Session()

# iOS User Agent
IOS_USER_AGENT = "Mozilla/5.0 (iPhone; CPU iPhone OS 16_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Mobile/15E148 Safari/604.1"

# UDisc's Parse Application ID
PARSE_APP_ID = "X7O7gSaOUxCv9cTAHSASADcGtaRq7Kf9a4gNA8rn"

# Endpoint where UDisc's Parse server lives
PARSE_ENDPOINT = "https://udisc.xyz/parse"

# HTTP headers that ensure requests will work with the Parse server
HEADERS = {
    "User-Agent": IOS_USER_AGENT,
    "X-Parse-Application-Id": PARSE_APP_ID,
    "X-Parse-Revocable-Session": "1",
}


def get(endpoint: str, params=None):
    """Perform a GET request against the Parse server."""
    return session.get(
        url=f"{PARSE_ENDPOINT}{endpoint}",
        headers=HEADERS,
        params=params,
    )


def post(endpoint: str, json=None):
    """Perform a POST request against the parse server."""
    return session.post(
        url=f"{PARSE_ENDPOINT}{endpoint}",
        headers=HEADERS,
        json=json,
    )


def set_session(session_token: str):
    """Set the session token header to the session token."""
    HEADERS["X-Parse-Session-Token"] = session_token
