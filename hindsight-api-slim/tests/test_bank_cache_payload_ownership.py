"""Cached JSON payloads belong to each caller rather than aliasing shared state."""

import asyncio
from typing import Any

import pytest
from hindsight_api.engine.bank_stats_cache import BankStatsCache


@pytest.mark.asyncio
@pytest.mark.parametrize("force_refresh", [False, True])
async def test_loader_and_returned_payload_do_not_modify_cached_data(force_refresh: bool) -> None:
    cache = BankStatsCache(ttl_seconds=60, max_entries=10)
    payload = {"config": {"labels": ["original"]}}

    async def loader() -> dict[str, Any]:
        return payload

    first = await cache.get_or_load("tenant", "bank", loader, force_refresh=force_refresh)
    payload["config"]["labels"].append("loader mutation")
    first["config"]["labels"].append("caller mutation")
    second = await cache.get_or_load("tenant", "bank", loader)
    assert second == {"config": {"labels": ["original"]}}
    second["config"]["labels"].append("hit mutation")
    assert await cache.get_or_load("tenant", "bank", loader) == {"config": {"labels": ["original"]}}


@pytest.mark.asyncio
async def test_coalesced_waiters_have_independent_nested_payloads() -> None:
    cache = BankStatsCache(ttl_seconds=60, max_entries=10)
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def loader() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return {"config": {"labels": ["original"]}}

    owner = asyncio.create_task(cache.get_or_load("tenant", "bank", loader))
    await started.wait()
    waiters = [asyncio.create_task(cache.get_or_load("tenant", "bank", loader)) for _ in range(2)]
    await asyncio.sleep(0)
    release.set()
    results = await asyncio.gather(owner, *waiters)
    assert calls == 1
    results[0]["config"]["labels"].append("owner mutation")
    assert results[1] == results[2] == {"config": {"labels": ["original"]}}
    results[1]["config"]["labels"].append("waiter mutation")
    assert results[2] == {"config": {"labels": ["original"]}}
    assert await cache.get_or_load("tenant", "bank", loader) == {"config": {"labels": ["original"]}}
