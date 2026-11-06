from __future__ import annotations

import difflib
from dataclasses import dataclass


@dataclass(frozen=True)
class ReconciliationDelta:
    needs_replace: bool
    text: str


class StreamReconciler:
    def __init__(self):
        self._accumulated = ""

    @property
    def current_text(self) -> str:
        return self._accumulated

    def append(self, chunk: str) -> str:
        self._accumulated += chunk
        return chunk

    def reconcile(self, snapshot: str) -> ReconciliationDelta:
        if self._accumulated == snapshot:
            return ReconciliationDelta(needs_replace=False, text="")

        if snapshot.startswith(self._accumulated):
            suffix = snapshot[len(self._accumulated) :]
            self._accumulated = snapshot
            return ReconciliationDelta(needs_replace=False, text=suffix)

        matcher = difflib.SequenceMatcher(None, self._accumulated, snapshot)
        ratio = matcher.quick_ratio()
        self._accumulated = snapshot
        if ratio > 0.6:
            return ReconciliationDelta(needs_replace=True, text=snapshot)
        return ReconciliationDelta(needs_replace=True, text=snapshot)


def reconcile_stream_text(streamed: str, authoritative: str) -> str:
    if streamed == authoritative:
        return ""
    if authoritative.startswith(streamed):
        return authoritative[len(streamed) :]
    return authoritative
