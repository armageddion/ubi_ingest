import json
from datetime import datetime, timedelta, timezone

import square_client

LOC = "LRGYZ8N0G4C4N"
OTHER = "LYJ6XMH6AEX01"


def _item(item_id, variations, **kwargs):
    return {
        "type": "ITEM",
        "id": item_id,
        "item_data": {"name": item_id, "variations": variations},
        **kwargs,
    }


def _var(var_id, **kwargs):
    data = kwargs.pop("data", {})
    return {"type": "ITEM_VARIATION", "id": var_id, "item_variation_data": data, **kwargs}


def test_item_availability_rules():
    objects = [
        _item("all", [_var("v1", present_at_all_locations=True)], present_at_all_locations=True),
        _item(
            "absent",
            [_var("v2", present_at_all_locations=True)],
            present_at_all_locations=True,
            absent_at_location_ids=[LOC],
        ),
        _item(
            "listed",
            [_var("v3", present_at_location_ids=[LOC])],
            present_at_location_ids=[LOC],
        ),
        _item(
            "elsewhere",
            [_var("v4", present_at_location_ids=[OTHER])],
            present_at_location_ids=[OTHER],
        ),
        _item("deleted", [_var("v5", present_at_all_locations=True)], present_at_all_locations=True, is_deleted=True),
    ]
    ids = [p["variationId"] for p in square_client.catalog_to_products(objects, LOC)]
    assert ids == ["v1", "v3"]


def test_variation_availability_is_independent_of_item():
    objects = [
        _item(
            "item",
            [
                _var("here", present_at_location_ids=[LOC]),
                _var("there", present_at_location_ids=[OTHER]),
            ],
            present_at_all_locations=True,
        )
    ]
    ids = [p["variationId"] for p in square_client.catalog_to_products(objects, LOC)]
    assert ids == ["here"]


def test_price_override_category_and_names():
    objects = [
        {"type": "CATEGORY", "id": "cat", "category_data": {"name": "Pokemon"}},
        _item(
            "booster",
            [
                _var(
                    "v1",
                    present_at_all_locations=True,
                    data={
                        "name": "Box",
                        "sku": "S1",
                        "upc": "123",
                        "price_money": {"amount": 499, "currency": "USD"},
                        "location_overrides": [
                            {
                                "location_id": LOC,
                                "price_override_money": {"amount": 599, "currency": "USD"},
                            }
                        ],
                    },
                ),
                _var("v2", present_at_all_locations=True, data={"name": "Pack"}),
            ],
            present_at_all_locations=True,
        ),
    ]
    objects[1]["item_data"]["category_id"] = "cat"
    first, second = square_client.catalog_to_products(objects, LOC)
    assert first["price"] == 5.99
    assert first["productName"] == "booster - Box"
    assert first["categoryName"] == "Pokemon"
    assert (first["sku"], first["upc"]) == ("S1", "123")
    assert second["price"] is None


def _write_tokens(tmp_path, expires_at):
    path = tmp_path / "tokens.json"
    path.write_text(
        json.dumps(
            {"M1": {"access_token": "old", "refresh_token": "r1", "expires_at": expires_at}}
        )
    )
    return path


def test_token_not_refreshed_when_far_from_expiry(tmp_path, monkeypatch):
    now = datetime(2026, 10, 2, tzinfo=timezone.utc)
    path = _write_tokens(tmp_path, (now + timedelta(days=20)).strftime("%Y-%m-%dT%H:%M:%SZ"))
    monkeypatch.setenv("SQUARE_TOKEN_FILE", str(path))

    def boom(*args, **kwargs):
        raise AssertionError("should not refresh")

    monkeypatch.setattr(square_client.requests, "post", boom)
    assert square_client.get_access_token("M1", now=now) == "old"


def test_token_refreshed_and_persisted_near_expiry(tmp_path, monkeypatch):
    now = datetime(2026, 10, 2, tzinfo=timezone.utc)
    path = _write_tokens(tmp_path, (now + timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ"))
    monkeypatch.setenv("SQUARE_TOKEN_FILE", str(path))
    monkeypatch.setenv("SQUARE_APPLICATION_ID", "app")
    monkeypatch.setenv("SQUARE_APPLICATION_SECRET", "secret")
    sent = {}

    class Response:
        ok = True

        def json(self):
            return {"access_token": "new", "expires_at": "2026-11-01T00:00:00Z"}

    def fake_post(url, json=None, timeout=None):
        sent.update(json)
        return Response()

    monkeypatch.setattr(square_client.requests, "post", fake_post)
    assert square_client.get_access_token("M1", now=now) == "new"
    assert sent["grant_type"] == "refresh_token" and sent["refresh_token"] == "r1"
    saved = json.loads(path.read_text())["M1"]
    assert saved["access_token"] == "new"
    assert saved["refresh_token"] == "r1"
    assert saved["expires_at"] == "2026-11-01T00:00:00Z"
    assert oct(path.stat().st_mode & 0o777) == "0o600"
