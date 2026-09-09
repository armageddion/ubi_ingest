import json

from square_callback import _make_state, create_app


def test_start_redirects_to_square(monkeypatch):
    monkeypatch.setenv("SQUARE_APPLICATION_ID", "app-id")
    monkeypatch.setenv("SQUARE_APPLICATION_SECRET", "app-secret")
    monkeypatch.setenv("SQUARE_OAUTH_STATE_SECRET", "state-secret")
    client = create_app().test_client()

    response = client.get("/square/oauth/start")

    assert response.status_code == 302
    assert response.location.startswith("https://connect.squareup.com/oauth2/authorize?")
    assert "client_id=app-id" in response.location


def test_callback_rejects_invalid_state(monkeypatch):
    monkeypatch.setenv("SQUARE_APPLICATION_ID", "app-id")
    monkeypatch.setenv("SQUARE_APPLICATION_SECRET", "app-secret")
    monkeypatch.setenv("SQUARE_OAUTH_STATE_SECRET", "state-secret")
    client = create_app().test_client()

    response = client.get("/square/oauth/callback?state=invalid&code=code")

    assert response.status_code == 400
    assert response.json["error"] == "Invalid or expired OAuth state"


def test_callback_exchanges_and_stores_tokens(monkeypatch, tmp_path):
    monkeypatch.setenv("SQUARE_APPLICATION_ID", "app-id")
    monkeypatch.setenv("SQUARE_APPLICATION_SECRET", "app-secret")
    monkeypatch.setenv("SQUARE_OAUTH_STATE_SECRET", "state-secret")
    token_file = tmp_path / "tokens.json"
    monkeypatch.setenv("SQUARE_TOKEN_FILE", str(token_file))

    class TokenResponse:
        ok = True

        def json(self):
            return {"merchant_id": "merchant-1", "access_token": "token"}

    monkeypatch.setattr(
        "square_callback.requests.post", lambda *args, **kwargs: TokenResponse()
    )
    state = _make_state("state-secret")
    response = create_app().test_client().get(
        f"/square/oauth/callback?state={state}&code=auth-code"
    )

    assert response.status_code == 200
    assert json.loads(token_file.read_text())["merchant-1"]["access_token"] == "token"