import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Any

DEFAULT_JSON_OFFLOAD_THRESHOLD = 256 * 1024


class BoundedWorkPool:
    def __init__(self, max_workers: int = 4):
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="webai-worker"
        )
        self._closed = False

    def submit(self, fn, *args, **kwargs):
        if self._closed:
            raise RuntimeError("work pool is closed")
        return self._executor.submit(fn, *args, **kwargs)

    async def run(self, fn, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, fn, *args, **kwargs)

    async def parse_json(
        self, data: bytes | str, threshold: int = DEFAULT_JSON_OFFLOAD_THRESHOLD
    ) -> Any:
        length = len(data)
        if length >= threshold:
            return await self.run(json.loads, data)
        return json.loads(data)

    def shutdown(self, wait: bool = True):
        self._closed = True
        self._executor.shutdown(wait=wait)
