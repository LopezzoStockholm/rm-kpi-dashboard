#!/usr/bin/env python3
"""
One-time OAuth2 setup for Fortnox API.
Run this ONCE after registering the app to get initial tokens.

Usage:
  1. Set CLIENT_ID and CLIENT_SECRET below
  2. Run: python3 fortnox_oauth_setup.py
  3. Open the printed URL in your browser
  4. Authorize the app — you'll get redirected to localhost with a code
  5. Copy the 'code' parameter from the URL
  6. Enter it when prompted
  7. Config file is saved to /opt/rm-infra/fortnox-config.json
"""
import json, sys
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from datetime import datetime, timedelta

CONFIG_PATH = "/opt/rm-infra/fortnox-config.json"

# ═══ FILL THESE IN ═══════════════════════════════════════
CLIENT_ID = ""       # From Fortnox developer portal
CLIENT_SECRET = ""   # From Fortnox developer portal
REDIRECT_URI = "https://rm-api.161-35-79-92.nip.io/oauth/callback"
# ═════════════════════════════════════════════════════════

SCOPES = "invoice+supplierinvoice+account+financialyear+sie"

def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("ERROR: Set CLIENT_ID and CLIENT_SECRET first!")
        sys.exit(1)

    # Step 1: Generate auth URL
    auth_url = (
        f"https://apps.fortnox.se/oauth-v1/auth"
        f"?client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&scope={SCOPES}"
        f"&state=rm_setup"
        f"&access_type=offline"
        f"&response_type=code"
    )

    print("\n" + "="*60)
    print("FORTNOX OAuth2 Setup")
    print("="*60)
    print(f"\n1. Open this URL in your browser:\n\n{auth_url}\n")
    print("2. Log in and authorize the app")
    print("3. You'll be redirected to a URL containing ?code=XXXX")
    print("4. Copy the code value and paste it below\n")

    code = input("Enter authorization code: ").strip()

    if not code:
        print("No code entered. Exiting.")
        sys.exit(1)

    # Step 2: Exchange code for tokens
    print("\nExchanging code for tokens...")
    data = urlencode({
        "grant_type": "authorization_code",
        "code": code,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
    }).encode()

    req = Request("https://apps.fortnox.se/oauth-v1/token", data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    resp = urlopen(req)
    token_data = json.loads(resp.read())

    # Step 3: Save config
    config = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "access_token": token_data["access_token"],
        "refresh_token": token_data["refresh_token"],
        "token_expires": (datetime.now() + timedelta(seconds=token_data.get("expires_in", 3600))).isoformat(),
    }

    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)

    print(f"\nTokens saved to {CONFIG_PATH}")
    print(f"Access token expires: {config['token_expires']}")
    print(f"\nDone! Fortnox sync will start running automatically via cron.")

if __name__ == "__main__":
    main()
