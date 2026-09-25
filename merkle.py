"""
Merkle tree over a node's chunk set, used for anti-entropy repair.
Two replicas can compare root hashes in O(1) and, on mismatch, walk down
to find exactly which keys diverged in O(log n) instead of diffing
the full chunk set.
"""

import hashlib
from typing import Dict, List, Optional


def _h(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class MerkleTree:
    def __init__(self, leaf_hashes: Dict[str, str]):
        """leaf_hashes: {chunk_key: content_checksum}"""
        self.leaves = dict(sorted(leaf_hashes.items()))
        self._levels: List[List[str]] = []
        self._build()

    def _build(self):
        if not self.leaves:
            self._levels = [[_h(b"empty")]]
            return
        level = [_h(f"{k}:{v}".encode()) for k, v in self.leaves.items()]
        self._levels = [level]
        while len(level) > 1:
            nxt = []
            for i in range(0, len(level), 2):
                left = level[i]
                right = level[i + 1] if i + 1 < len(level) else left
                nxt.append(_h((left + right).encode()))
            level = nxt
            self._levels.append(level)

    @property
    def root(self) -> str:
        return self._levels[-1][0]

    def diverged_keys(self, other: "MerkleTree") -> List[str]:
        """Cheap path: if roots match, nothing diverged. Otherwise fall back
        to comparing leaf sets directly (sufficient for demo scale; a real
        implementation would walk matching subtrees to prune the comparison)."""
        if self.root == other.root:
            return []
        keys = set(self.leaves) | set(other.leaves)
        diverged = [
            k for k in keys
            if self.leaves.get(k) != other.leaves.get(k)
        ]
        return sorted(diverged)
