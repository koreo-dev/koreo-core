import asyncio
import logging

import kr8s.asyncio

logger = logging.getLogger("koreo.resource_function.reconcile")

LOOKUP_TIMEOUT = 15

_plural_map: dict[str, str] = {}

_lookup_locks: dict[str, asyncio.Event] = {}


async def get_plural_kind(
    api: kr8s.asyncio.Api, kind: str, api_version: str
) -> str | None:
    if api_version == "v1":
        lookup_kind = f"{kind}"
    else:
        lookup_kind = f"{kind}.{api_version}"

    if lookup_kind in _plural_map:
        return _plural_map[lookup_kind]

    lookup_lock = _lookup_locks.get(lookup_kind)
    if lookup_lock:
        try:
            await asyncio.wait_for(lookup_lock.wait(), timeout=LOOKUP_TIMEOUT)
        except asyncio.TimeoutError:
            pass

        if lookup_kind in _plural_map:
            return _plural_map[lookup_kind]

        raise Exception(f"Waiting on {lookup_kind} failed.")

    lookup_lock = asyncio.Event()
    _lookup_locks[lookup_kind] = lookup_lock

    for _ in range(3):
        try:
            async with asyncio.timeout(LOOKUP_TIMEOUT):
                try:
                    (_, plural_kind, _) = await api.lookup_kind(lookup_kind)
                    break
                except ValueError:
                    del _lookup_locks[lookup_kind]
                    logger.error(f"Failed to find Kind (`{lookup_kind}`) information.")
                    return None
        except asyncio.TimeoutError:
            continue
        except:
            del _lookup_locks[lookup_kind]
            raise
    else:
        del _lookup_locks[lookup_kind]
        raise Exception(
            f"Too many failed attempts to find plural kind for {lookup_kind} failed."
        )

    _plural_map[lookup_kind] = plural_kind

    lookup_lock.set()

    return plural_kind


def _reset():
    """Helper for unit testing; not intended for usage in normal code."""
    _plural_map.clear()

    for lock in _lookup_locks.values():
        lock.set()

    _lookup_locks.clear()
