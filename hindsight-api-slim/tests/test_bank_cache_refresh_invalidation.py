"""Forced refreshes respect invalidation while their loader is in flight."""

import asyncio
from typing import Any

import pytest

from hindsight_api.engine.bank_stats_cache import BankStatsCache


@pytest.mark.asyncio
@pytest.mark.parametrize("clear_all", [False, True])
async def test_invalidated_forced_refresh_cannot_repopulate_old_data(clear_all: bool) -> None:
    cache = BankStatsCache(ttl_seconds=60, max_entries=10)
    started = asyncio.Event()
    release = asyncio.Event()

    async def old_loader() -> dict[str, Any]:
        started.set()
        await release.wait()
        return {"profile": "before update"}

    old = asyncio.create_task(cache.get_or_load("tenant", "bank", old_loader, force_refresh=True))
    await started.wait()
    if clear_all:
        await cache.clear()
    else:
        await cache.invalidate("tenant", "bank")

    async def current_loader() -> dict[str, Any]:
        return {"profile": "after update"}

    assert await cache.get_or_load("tenant", "bank", current_loader) == {"profile": "after update"}
    release.set()
    assert await old == {"profile": "before update"}
    assert await cache.get_or_load("tenant", "bank", current_loader) == {"profile": "after update"}


@pytest.mark.asyncio
async def test_later_forced_refresh_remains_cached_when_older_refresh_finishes() -> None:
    cache = BankStatsCache(ttl_seconds=60, max_entries=10)
    started = asyncio.Event()
    release = asyncio.Event()

    async def old_loader() -> dict[str, Any]:
        started.set()
        await release.wait()
        return {"profile": "older"}

    old = asyncio.create_task(cache.get_or_load("tenant", "bank", old_loader, force_refresh=True))
    await started.wait()

    async def current_loader() -> dict[str, Any]:
        return {"profile": "newer"}

    assert await cache.get_or_load("tenant", "bank", current_loader, force_refresh=True) == {"profile": "newer"}
    release.set()
    await old
    assert await cache.get_or_load("tenant", "bank", current_loader) == {"profile": "newer"}
