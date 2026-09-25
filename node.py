"""
StorageNode: owns local "disk" (in-memory dict standing in for it),
stores immutable content-addressed chunks with checksums, and can be
told to go offline/online or have its data corrupted to simulate faults.
"""

import hashlib
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from merkle import MerkleTree


def checksum(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass
class StoredChunk:
    data: bytes
    checksum: str
    version: Dict[str, int]        # vector clock: {node_id: counter}
    written_at: float = field(default_factory=time.time)
    is_hint: bool = False          # True if this is a hinted-handoff stand-in
    hint_for: Optional[str] = None  # real owner node id, if is_hint


class NodeUnavailable(Exception):
    pass


class StorageNode:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self._store: Dict[str, StoredChunk] = {}
        self._lock = threading.RLock()
        self.alive = True
        # per-node request latency stats, used by the coordinator for
        # load-aware read routing
        self.recent_latencies = []

    # ---- fault injection (simulated) ----
    def kill(self):
        self.alive = False

    def revive(self):
        self.alive = True

    def corrupt_chunk(self, key: str):
        """Simulate silent bit rot: flip the stored bytes without updating checksum."""
        with self._lock:
            if key in self._store:
                c = self._store[key]
                bad = bytearray(c.data)
                bad[0] ^= 0xFF
                self._store[key] = StoredChunk(
                    data=bytes(bad), checksum=c.checksum,
                    version=c.version, written_at=c.written_at,
                    is_hint=c.is_hint, hint_for=c.hint_for,
                )

    # ---- core ops ----
    def put(self, key: str, data: bytes, version: Dict[str, int],
            is_hint: bool = False, hint_for: Optional[str] = None) -> str:
        if not self.alive:
            raise NodeUnavailable(self.node_id)
        cs = checksum(data)
        with self._lock:
            self._store[key] = StoredChunk(
                data=data, checksum=cs, version=version,
                is_hint=is_hint, hint_for=hint_for,
            )
        return cs

    def get(self, key: str) -> StoredChunk:
        if not self.alive:
            raise NodeUnavailable(self.node_id)
        with self._lock:
            if key not in self._store:
                raise KeyError(key)
            chunk = self._store[key]
        # integrity check on every read
        actual = checksum(chunk.data)
        if actual != chunk.checksum:
            raise IntegrityError(self.node_id, key, expected=chunk.checksum, actual=actual)
        return chunk

    def delete(self, key: str):
        with self._lock:
            self._store.pop(key, None)

    def has(self, key: str) -> bool:
        with self._lock:
            return key in self._store

    def hinted_keys(self):
        with self._lock:
            return [k for k, c in self._store.items() if c.is_hint]

    def local_keys(self):
        with self._lock:
            return [k for k, c in self._store.items() if not c.is_hint]

    def merkle_tree(self) -> MerkleTree:
        with self._lock:
            leaves = {k: c.checksum for k, c in self._store.items() if not c.is_hint}
        return MerkleTree(leaves)

    def scrub(self) -> list:
        """Background integrity scan: re-checksum everything locally stored.
        Returns list of keys found corrupted."""
        bad = []
        with self._lock:
            items = list(self._store.items())
        for key, chunk in items:
            if checksum(chunk.data) != chunk.checksum:
                bad.append(key)
        return bad


class IntegrityError(Exception):
    def __init__(self, node_id, key, expected, actual):
        super().__init__(f"corruption on {node_id} for {key}: expected {expected[:8]}, got {actual[:8]}")
        self.node_id = node_id
        self.key = key
