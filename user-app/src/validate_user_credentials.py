import requests

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


def validate_udisc_credentials(username: str, password: str):
    """Validate UDisc credentials by attempting to login to the UDisc API"""
    try:
        response = requests.post(
            url=f"{PARSE_ENDPOINT}/login",
            headers=HEADERS,
            json={
                "username": username,
                "password": password
            }
        )

        if response.ok:
            print(f"UDisc login validation succeeded for user: {username}")
            return True, "Credentials validated successfully"
        else:
            error_data = response.json()
            error_message = error_data.get("error", "Unknown error")
            print(
                f"UDisc login validation failed for user: {username} - {error_message}")
            return False, f"Invalid UDisc credentials: {error_message}"

    except Exception as e:
        print(f"Error during UDisc login validation: {e}")
        return False, f"Error validating credentials: {str(e)}"
