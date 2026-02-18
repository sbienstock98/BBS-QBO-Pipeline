"""
One-time OAuth2 authorization code flow for onboarding a new QBO client.

Usage:
    python -m auth.qbo_auth_flow --client-id pilot_001

This starts a local HTTP server, opens the Intuit authorization page in
your browser, and captures the callback with the authorization code.
"""

import argparse
import http.server
import json
import logging
import threading
import urllib.parse
import webbrowser

import requests

from config.settings import Settings
from auth.oauth_manager import OAuthManager

logger = logging.getLogger(__name__)

AUTHORIZATION_URL = "https://appcenter.intuit.com/connect/oauth2"
TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
SCOPES = "com.intuit.quickbooks.accounting"


class OAuthCallbackHandler(http.server.BaseHTTPRequestHandler):
    """HTTP handler that captures the OAuth callback."""

    auth_code = None
    realm_id = None
    error = None

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if "code" in params:
            OAuthCallbackHandler.auth_code = params["code"][0]
            OAuthCallbackHandler.realm_id = params.get("realmId", [None])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"<h1>Authorization successful!</h1>"
                b"<p>You can close this window and return to the terminal.</p>"
            )
        else:
            OAuthCallbackHandler.error = params.get("error", ["unknown"])[0]
            self.send_response(400)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(
                f"<h1>Authorization failed: {OAuthCallbackHandler.error}</h1>".encode()
            )

    def log_message(self, format, *args):
        pass  # Suppress default HTTP logs


def run_oauth_flow(settings: Settings, client_id: str):
    """Run the complete OAuth2 authorization code flow."""
    # Parse redirect URI to get host and port
    parsed_redirect = urllib.parse.urlparse(settings.QBO_REDIRECT_URI)
    host = parsed_redirect.hostname or "localhost"
    port = parsed_redirect.port or 8080

    # Build authorization URL
    auth_params = {
        "client_id": settings.QBO_CLIENT_ID,
        "response_type": "code",
        "scope": SCOPES,
        "redirect_uri": settings.QBO_REDIRECT_URI,
        "state": client_id,
    }
    auth_url = f"{AUTHORIZATION_URL}?{urllib.parse.urlencode(auth_params)}"

    # Start local callback server
    server = http.server.HTTPServer((host, port), OAuthCallbackHandler)
    server_thread = threading.Thread(target=server.handle_request)
    server_thread.start()

    print(f"\nOpening browser for QuickBooks authorization...")
    print(f"If the browser doesn't open, visit:\n{auth_url}\n")
    webbrowser.open(auth_url)

    # Wait for callback
    server_thread.join(timeout=120)
    server.server_close()

    if OAuthCallbackHandler.error:
        raise RuntimeError(
            f"OAuth authorization failed: {OAuthCallbackHandler.error}"
        )

    if not OAuthCallbackHandler.auth_code:
        raise RuntimeError("No authorization code received (timed out after 120s)")

    auth_code = OAuthCallbackHandler.auth_code
    realm_id = OAuthCallbackHandler.realm_id

    print(f"Authorization code received for realm: {realm_id}")

    # Exchange authorization code for tokens
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": auth_code,
            "redirect_uri": settings.QBO_REDIRECT_URI,
        },
        auth=(settings.QBO_CLIENT_ID, settings.QBO_CLIENT_SECRET),
        timeout=30,
    )
    response.raise_for_status()
    tokens = response.json()

    # Store tokens
    oauth = OAuthManager(settings, client_id)
    oauth.store_initial_tokens(
        access_token=tokens["access_token"],
        refresh_token=tokens["refresh_token"],
        realm_id=realm_id,
        expires_in=tokens.get("expires_in", 3600),
    )

    print(f"Tokens stored successfully for client: {client_id}")
    print(f"  Realm ID: {realm_id}")
    print(f"  Access token expires in: {tokens.get('expires_in', 3600)}s")
    print(f"  Refresh token expires in: {tokens.get('x_refresh_token_expires_in', 8726400)}s")

    return realm_id


def main():
    parser = argparse.ArgumentParser(
        description="Authorize a QuickBooks Online company via OAuth2"
    )
    parser.add_argument(
        "--client-id",
        required=True,
        help="Client identifier (e.g., 'pilot_001')",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    settings = Settings()

    if not settings.QBO_CLIENT_ID or not settings.QBO_CLIENT_SECRET:
        print("ERROR: Set QBO_CLIENT_ID and QBO_CLIENT_SECRET in .env file")
        return

    run_oauth_flow(settings, args.client_id)


if __name__ == "__main__":
    main()
