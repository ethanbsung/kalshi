import asyncio

from kalshi_bot.events import InMemoryEventBus, QuoteUpdateEvent, SpotTickEvent


def test_in_memory_event_bus_filters_by_event_type() -> None:
    async def _run() -> None:
        bus = InMemoryEventBus()
        spot_q = bus.subscribe(event_types={"spot_tick"})
        all_q = bus.subscribe()

        await bus.publish(
            SpotTickEvent(
                source="svc_spot_ingest",
                payload={
                    "ts": 1_700_000_001,
                    "product_id": "BTC-USD",
                    "price": 51000.0,
                },
            )
        )
        await bus.publish(
            QuoteUpdateEvent(
                source="svc_quote_ingest",
                payload={
                    "ts": 1_700_000_002,
                    "market_id": "KXBTC-TEST",
                },
            )
        )

        spot_only = await asyncio.wait_for(spot_q.get(), timeout=1.0)
        first_all = await asyncio.wait_for(all_q.get(), timeout=1.0)
        second_all = await asyncio.wait_for(all_q.get(), timeout=1.0)

        assert spot_only.event_type == "spot_tick"
        assert first_all.event_type == "spot_tick"
        assert second_all.event_type == "quote_update"

    asyncio.run(_run())


def test_in_memory_event_bus_unsubscribe_stops_delivery() -> None:
    async def _run() -> None:
        bus = InMemoryEventBus()
        queue = bus.subscribe()
        bus.unsubscribe(queue)

        await bus.publish(
            SpotTickEvent(
                source="svc_spot_ingest",
                payload={
                    "ts": 1_700_000_003,
                    "product_id": "BTC-USD",
                    "price": 52000.0,
                },
            )
        )

        assert queue.empty()

    asyncio.run(_run())
