import hashlib
import hmac
import json
import os
import secrets
import time
from functools import wraps
from urllib.parse import urlencode

import requests
from flask import Flask, jsonify, redirect, request


DEFAULT_REDIRECT_URI = (
    "https://149.248.54.250.littl31.com/square/oauth/callback"
)
SQUARE_AUTHORIZE_URL = "https://connect.squareup.com/oauth2/authorize"
SQUARE_TOKEN_URL = "https://connect.squareup.com/oauth2/token"


def _setting(name, default=None):
    value = os.getenv(name, default)
    return value.strip() if isinstance(value, str) else value


def _sign_state(timestamp, nonce, secret):
    message = f"{timestamp}.{nonce}".encode("ascii")
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def _make_state(secret):
    timestamp = str(int(time.time()))
    nonce = secrets.token_urlsafe(32)
    signature = _sign_state(timestamp, nonce, secret)
    return f"{timestamp}.{nonce}.{signature}"


def _valid_state(state, secret, max_age=600):
    try:
        timestamp, nonce, signature = state.split(".", 2)
        issued_at = int(timestamp)
    except (AttributeError, ValueError):
        return False
    if abs(time.time() - issued_at) > max_age:
        return False
    expected = _sign_state(timestamp, nonce, secret)
    return hmac.compare_digest(signature, expected)


def _require_settings(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        required = {
            "SQUARE_APPLICATION_ID": _setting("SQUARE_APPLICATION_ID"),
            "SQUARE_APPLICATION_SECRET": _setting("SQUARE_APPLICATION_SECRET"),
            "SQUARE_OAUTH_STATE_SECRET": _setting("SQUARE_OAUTH_STATE_SECRET"),
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            return jsonify(error="Missing Square configuration", settings=missing), 503
        return function(*args, **kwargs)

    return wrapped


def create_app():
    app = Flask(__name__)

    @app.get("/square/oauth/start")
    @_require_settings
    def square_oauth_start():
        state = _make_state(_setting("SQUARE_OAUTH_STATE_SECRET"))
        query = urlencode(
            {
                "client_id": _setting("SQUARE_APPLICATION_ID"),
                "scope": _setting(
                    "SQUARE_OAUTH_SCOPES", "ITEMS_READ MERCHANT_PROFILE_READ"
                ),
                "session": "false",
                "state": state,
            }
        )
        return redirect(f"{SQUARE_AUTHORIZE_URL}?{query}")

    @app.get("/square/oauth/callback")
    @_require_settings
    def square_oauth_callback():
        state = request.args.get("state")
        if not _valid_state(state, _setting("SQUARE_OAUTH_STATE_SECRET")):
            return jsonify(error="Invalid or expired OAuth state"), 400
        if request.args.get("error"):
            return jsonify(
                error=request.args["error"],
                description=request.args.get("error_description", ""),
            ), 400
        code = request.args.get("code")
        if not code:
            return jsonify(error="Square authorization code is missing"), 400

        response = requests.post(
            SQUARE_TOKEN_URL,
            json={
                "client_id": _setting("SQUARE_APPLICATION_ID"),
                "client_secret": _setting("SQUARE_APPLICATION_SECRET"),
                "code": code,
                "grant_type": "authorization_code",
            },
            timeout=30,
        )
        if not response.ok:
            return jsonify(error="Square token exchange failed"), 502

        token_data = response.json()
        merchant_id = token_data.get("merchant_id", "unknown")
        token_path = _setting("SQUARE_TOKEN_FILE", "square_tokens.json")
        tokens = {}
        if os.path.exists(token_path):
            with open(token_path, "r", encoding="utf-8") as token_file:
                tokens = json.load(token_file)
        tokens[merchant_id] = token_data
        with open(token_path, "w", encoding="utf-8") as token_file:
            json.dump(tokens, token_file, indent=2)
        os.chmod(token_path, 0o600)
        return jsonify(message="Square authorization completed", merchant_id=merchant_id)

    return app


if __name__ == "__main__":
    create_app().run(
        host=_setting("SQUARE_OAUTH_HOST", "127.0.0.1"),
        port=int(_setting("SQUARE_OAUTH_PORT", "8080")),
    )