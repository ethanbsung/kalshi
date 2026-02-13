import asyncio
import json

from kalshi_bot.events import EventPublisher, SpotTickEvent


def test_event_publisher_jsonl_only(tmp_path) -> None:
    path = tmp_path / "events.jsonl"

    async def _run() -> None:
        publisher = await EventPublisher.create(jsonl_path=path)
        try:
            await publisher.publish(
                SpotTickEvent(
                    source="test",
                    payload={
                        "ts": 1_700_000_000,
                        "product_id": "BTC-USD",
                        "price": 50000.0,
                    },
                )
            )
        finally:
            await publisher.close()

    asyncio.run(_run())
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["event_type"] == "spot_tick"


def test_event_publisher_disabled_is_noop() -> None:
    async def _run() -> None:
        publisher = await EventPublisher.create()
        assert not publisher.enabled
        try:
            await publisher.publish(
                SpotTickEvent(
                    source="test",
                    payload={
                        "ts": 1_700_000_001,
                        "product_id": "BTC-USD",
                        "price": 50001.0,
                    },
                )
            )
        finally:
            await publisher.close()

    asyncio.run(_run())
