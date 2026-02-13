from pydantic import ValidationError

from kalshi_bot.events import (
    QuoteUpdateEvent,
    SpotTickEvent,
    parse_event_dict,
    schema_version_for_event,
    subject_for_event,
)


def test_spot_tick_event_generates_idempotency_key() -> None:
    event = SpotTickEvent(
        source="svc_spot_ingest",
        payload={
            "ts": 1_700_000_000,
            "product_id": "BTC-USD",
            "price": 50_000.0,
            "sequence_num": 42,
        },
    )
    assert event.idempotency_key
    assert event.idempotency_key.startswith("spot_tick:v1:")


def test_schema_version_is_enforced() -> None:
    try:
        SpotTickEvent(
            source="svc_spot_ingest",
            schema_version=2,
            payload={
                "ts": 1_700_000_000,
                "product_id": "BTC-USD",
                "price": 50_000.0,
            },
        )
    except ValidationError:
        return
    raise AssertionError("Expected schema-version validation failure")


def test_subject_and_schema_lookup() -> None:
    assert subject_for_event("spot_tick") == "market.spot_ticks"
    assert schema_version_for_event("spot_tick") == 1


def test_parse_event_dict_round_trip() -> None:
    raw = {
        "event_type": "spot_tick",
        "schema_version": 1,
        "ts_event": 1_700_000_001,
        "source": "svc_spot_ingest",
        "payload": {
            "ts": 1_700_000_000,
            "product_id": "BTC-USD",
            "price": 50_000.0,
            "sequence_num": 123,
        },
    }
    event = parse_event_dict(raw)
    assert event.event_type == "spot_tick"
    assert event.idempotency_key


def test_quote_idempotency_falls_back_when_source_msg_id_missing() -> None:
    first = QuoteUpdateEvent(
        source="svc_quote_ingest",
        payload={
            "ts": 1_700_000_100,
            "market_id": "KXBTC-TEST",
            "yes_bid": 45.0,
            "yes_ask": 50.0,
        },
    )
    second = QuoteUpdateEvent(
        source="svc_quote_ingest",
        payload={
            "ts": 1_700_000_100,
            "market_id": "KXBTC-TEST",
            "yes_bid": 46.0,
            "yes_ask": 50.0,
        },
    )
    assert first.idempotency_key != second.idempotency_key
