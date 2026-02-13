import asyncio

from kalshi_bot.events import SpotTickEvent
from kalshi_bot.persistence import InMemoryEventRepository, PersistenceService


def test_persistence_service_idempotent_insert() -> None:
    repo = InMemoryEventRepository()
    svc = PersistenceService(repo)
    svc.ensure_schema()

    event = SpotTickEvent(
        source="svc_spot_ingest",
        payload={
            "ts": 1_700_000_000,
            "product_id": "BTC-USD",
            "price": 50_000.0,
            "sequence_num": 100,
        },
    )

    first = svc.persist_event(event)
    second = svc.persist_event(event)

    assert first is True
    assert second is False
    assert repo.count() == 1


def test_persistence_service_queue_consumer_processes_events() -> None:
    async def _run() -> None:
        repo = InMemoryEventRepository()
        svc = PersistenceService(repo)
        svc.ensure_schema()

        queue: asyncio.Queue[SpotTickEvent] = asyncio.Queue()
        stop_event = asyncio.Event()

        await queue.put(
            SpotTickEvent(
                source="svc_spot_ingest",
                payload={
                    "ts": 1_700_000_001,
                    "product_id": "BTC-USD",
                    "price": 50_001.0,
                    "sequence_num": 101,
                },
            )
        )

        async def _stop() -> None:
            await asyncio.sleep(0.05)
            stop_event.set()

        consumer = asyncio.create_task(
            svc.run_queue_consumer(
                queue,
                stop_event=stop_event,
                poll_timeout_seconds=0.01,
            )
        )
        stopper = asyncio.create_task(_stop())

        await asyncio.gather(consumer, stopper)

        stats = consumer.result()
        assert stats.processed >= 1
        assert stats.inserted >= 1
        assert repo.count() == 1

    asyncio.run(_run())
