import asyncio
import logging

from kalshi_bot.kalshi.rest_client import KalshiRestClient


def test_list_markets_max_pages_stops(monkeypatch, caplog):
    responses = [
        {"markets": [{"ticker": "A"}], "cursor": "next"},
        {"markets": [{"ticker": "B"}], "cursor": "more"},
    ]
    calls = []

    async def fake_request(self, method, path, params, end_time=None):
        calls.append(params)
        return responses[len(calls) - 1]

    client = KalshiRestClient(
        base_url="https://example.com",
        api_key_id=None,
        private_key_path=None,
        logger=logging.getLogger("kalshi_test"),
    )
    monkeypatch.setattr(KalshiRestClient, "_request_json", fake_request)

    with caplog.at_level(logging.INFO):
        markets = asyncio.run(
            client.list_markets(limit=1, max_pages=1, status="open")
        )

    assert markets == [{"ticker": "A"}]
    assert len(calls) == 1
    assert any(record.msg == "kalshi_rest_pagination_stop" for record in caplog.records)
