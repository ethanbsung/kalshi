import asyncio
import json

from kalshi_bot.events import JsonlEventSink, SpotTickEvent


def test_jsonl_event_sink_writes_lines(tmp_path):
    path = tmp_path / "events.jsonl"

    async def _run() -> None:
        sink = JsonlEventSink(path)
        await sink.publish(
            SpotTickEvent(
                source="test",
                payload={
                    "ts": 1_700_000_000,
                    "product_id": "BTC-USD",
                    "price": 50000.0,
                },
            )
        )
        await sink.publish_dict(
            {
                "event_type": "custom",
                "schema_version": 1,
                "ts_event": 1_700_000_001,
                "source": "test",
                "payload": {"ok": True},
            }
        )

    asyncio.run(_run())
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    second = json.loads(lines[1])
    assert first["event_type"] == "spot_tick"
    assert second["event_type"] == "custom"
