from pydantic import ValidationError

from kalshi_bot.events import QuoteUpdateEvent, SpotTickEvent


def test_spot_tick_event_schema_validates() -> None:
    event = SpotTickEvent(
        source="svc_spot_ingest",
        payload={
            "ts": 1_700_000_000,
            "product_id": "BTC-USD",
            "price": 50000.0,
            "best_bid": 49999.0,
            "best_ask": 50001.0,
            "sequence_num": 123,
        },
    )
    assert event.event_type == "spot_tick"
    assert event.payload.product_id == "BTC-USD"


def test_quote_update_event_rejects_unknown_fields() -> None:
    try:
        QuoteUpdateEvent(
            source="svc_quote_ingest",
            payload={
                "ts": 1_700_000_000,
                "market_id": "KXBTC-TEST",
                "yes_bid": 45.0,
                "unexpected": 1,
            },
        )
    except ValidationError:
        return
    raise AssertionError("Expected QuoteUpdateEvent validation to fail")
