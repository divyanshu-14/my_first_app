"""
Metadata layer. In production this is a small Raft/Paxos cluster so all
gateways agree on cluster membership and object versions; here it's a
single in-memory store guarded by a lock, which is the correct simplification
for a single-process demo — the important thing is that *all* placement and
versioning decisions go through one authority, not that the authority is
literally Raft.
"""

import threading
from typing import Dict, List, Optional


class MetadataStore:
    def __init__(self):
        self._lock = threading.RLock()
        self._object_versions: Dict[str, Dict[str, int]] = {}  # key -> vector clock
        self._object_replicas: Dict[str, List[str]] = {}        # key -> node ids holding it
        self._suspected_down: set = set()   # locally unreachable, not yet confirmed
        self._confirmed_down: set = set()   # cluster-quorum agrees this node is dead

    def bump_version(self, key: str, coordinator_id: str) -> Dict[str, int]:
        with self._lock:
            vc = dict(self._object_versions.get(key, {}))
            vc[coordinator_id] = vc.get(coordinator_id, 0) + 1
            self._object_versions[key] = vc
            return dict(vc)

    def get_version(self, key: str) -> Optional[Dict[str, int]]:
        with self._lock:
            return dict(self._object_versions[key]) if key in self._object_versions else None

    def set_replicas(self, key: str, nodes: List[str]):
        with self._lock:
            self._object_replicas[key] = list(nodes)

    def get_replicas(self, key: str) -> List[str]:
        with self._lock:
            return list(self._object_replicas.get(key, []))

    def mark_suspected(self, node_id: str):
        with self._lock:
            self._suspected_down.add(node_id)

    def clear_suspected(self, node_id: str):
        with self._lock:
            self._suspected_down.discard(node_id)

    def confirm_down(self, node_id: str):
        """Called once a quorum of gateways/nodes agree a node is truly dead
        (not just unreachable from one vantage point) -- this is what
        distinguishes a real failure from a partial network partition."""
        with self._lock:
            self._confirmed_down.add(node_id)

    def confirm_up(self, node_id: str):
        with self._lock:
            self._confirmed_down.discard(node_id)
            self._suspected_down.discard(node_id)

    def is_confirmed_down(self, node_id: str) -> bool:
        with self._lock:
            return node_id in self._confirmed_down

    def all_object_keys(self) -> List[str]:
        with self._lock:
            return list(self._object_replicas.keys())


def compare_versions(a: Dict[str, int], b: Dict[str, int]) -> str:
    """Vector clock comparison. Returns 'a', 'b', 'equal', or 'concurrent'."""
    a_keys, b_keys = set(a), set(b)
    all_keys = a_keys | b_keys
    a_geq = all(a.get(k, 0) >= b.get(k, 0) for k in all_keys)
    b_geq = all(b.get(k, 0) >= a.get(k, 0) for k in all_keys)
    if a_geq and b_geq:
        return "equal"
    if a_geq:
        return "a"
    if b_geq:
        return "b"
    return "concurrent"  # sibling versions -- needs merge/client resolution
