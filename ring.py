"""
Consistent hashing ring with virtual nodes.

Each physical node is mapped to many points on a 2^160 hash ring (via SHA-1).
Placement of a key = walk clockwise from hash(key), collecting the first N
distinct PHYSICAL nodes encountered (skipping virtual-node collisions that
map back to a node already chosen).
"""

import bisect
import hashlib
from typing import Dict, List


def _hash(key: str) -> int:
    return int(hashlib.sha1(key.encode("utf-8")).hexdigest(), 16)


class HashRing:
    def __init__(self, virtual_nodes: int = 150):
        self.virtual_nodes = virtual_nodes
        self._ring: Dict[int, str] = {}      # hash position -> physical node id
        self._sorted_keys: List[int] = []     # kept sorted for bisect lookup
        self._members: set = set()

    def add_node(self, node_id: str) -> None:
        if node_id in self._members:
            return
        self._members.add(node_id)
        for i in range(self.virtual_nodes):
            pos = _hash(f"{node_id}#vn{i}")
            self._ring[pos] = node_id
            bisect.insort(self._sorted_keys, pos)

    def remove_node(self, node_id: str) -> None:
        if node_id not in self._members:
            return
        self._members.discard(node_id)
        for i in range(self.virtual_nodes):
            pos = _hash(f"{node_id}#vn{i}")
            if pos in self._ring:
                del self._ring[pos]
                idx = bisect.bisect_left(self._sorted_keys, pos)
                if idx < len(self._sorted_keys) and self._sorted_keys[idx] == pos:
                    self._sorted_keys.pop(idx)

    def get_replicas(self, key: str, n: int) -> List[str]:
        """Return the first n distinct physical nodes clockwise from hash(key)."""
        if not self._sorted_keys:
            return []
        n = min(n, len(self._members))
        h = _hash(key)
        idx = bisect.bisect_right(self._sorted_keys, h) % len(self._sorted_keys)

        chosen: List[str] = []
        seen: set = set()
        steps = 0
        total_positions = len(self._sorted_keys)
        while len(chosen) < n and steps < total_positions:
            pos = self._sorted_keys[(idx + steps) % total_positions]
            node = self._ring[pos]
            if node not in seen:
                seen.add(node)
                chosen.append(node)
            steps += 1
        return chosen

    def members(self) -> List[str]:
        return sorted(self._members)
