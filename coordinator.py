"""
Cluster: the gateway/coordinator logic. This is where placement (ring),
membership (metadata), and per-node storage (StorageNode) come together
to implement:
  - configurable N/W/R quorum writes and reads
  - sloppy quorum + hinted handoff when a target node is down
  - read repair (stale replicas patched opportunistically on read)
  - active anti-entropy repair (Merkle-tree diff, runs in background)
  - re-replication when a node is confirmed permanently dead
  - throttled rebalancing when membership changes
"""

import random
import threading
import time
from typing import Dict, List, Optional

from ring import HashRing
from node import StorageNode, NodeUnavailable, IntegrityError, checksum
from metadata import MetadataStore, compare_versions
from failure_detector import PhiAccrualDetector


class QuorumNotReached(Exception):
    pass


class Cluster:
    def __init__(self, n: int = 3, w: int = 2, r: int = 2, virtual_nodes: int = 100):
        assert w <= n and r <= n, "W and R must be <= N"
        self.n, self.w, self.r = n, w, r
        self.ring = HashRing(virtual_nodes=virtual_nodes)
        self.nodes: Dict[str, StorageNode] = {}
        self.meta = MetadataStore()
        self.fd = PhiAccrualDetector()
        self._lock = threading.RLock()
        self.log: List[str] = []
        self._rebalance_budget_per_tick = 5  # throttle: max chunks moved per tick

    # ---------- membership ----------
    def add_node(self, node_id: str, rebalance: bool = True):
        node = StorageNode(node_id)
        with self._lock:
            self.nodes[node_id] = node
            self.ring.add_node(node_id)
            self.meta.confirm_up(node_id)
        self._log(f"[membership] node {node_id} joined")
        self.fd.heartbeat(node_id)
        if rebalance:
            self._rebalance()

    def kill_node(self, node_id: str):
        """Simulate a hard failure -- node stops responding entirely."""
        self.nodes[node_id].kill()
        self._log(f"[fault] node {node_id} went offline")

    def revive_node(self, node_id: str):
        self.nodes[node_id].revive()
        self.fd.heartbeat(node_id)
        self.meta.confirm_up(node_id)
        self._log(f"[fault] node {node_id} back online")

    def remove_node_permanently(self, node_id: str):
        """Cluster-quorum has confirmed the node is truly gone (not a
        transient partition) -- evict from the ring and trigger re-replication."""
        with self._lock:
            self.meta.confirm_down(node_id)
            self.ring.remove_node(node_id)
            del self.nodes[node_id]
        self._log(f"[membership] node {node_id} confirmed dead, evicted from ring")
        self._reReplicate_owned_by(node_id)

    def _log(self, msg: str):
        self.log.append(msg)

    # ---------- write path ----------
    def put(self, key: str, data: bytes) -> Dict[str, int]:
        targets = self.ring.get_replicas(key, self.n)
        if not targets:
            raise RuntimeError("no nodes in cluster")

        version = self.meta.bump_version(key, coordinator_id="gateway")
        acks = 0
        hinted = []

        for target in targets:
            node = self.nodes[target]
            if node.alive:
                try:
                    node.put(key, data, version)
                    acks += 1
                    continue
                except NodeUnavailable:
                    pass
            # target down -> sloppy quorum: hand off to next healthy node
            # not already in the target set (hinted handoff)
            fallback = self._pick_hint_target(exclude=set(targets))
            if fallback:
                self.nodes[fallback].put(key, data, version, is_hint=True, hint_for=target)
                hinted.append((fallback, target))
                acks += 1  # counts toward durability -- data IS stored, just not on the canonical node yet
                self._log(f"[hinted-handoff] {target} down, wrote hint to {fallback} for key={key}")

        self.meta.set_replicas(key, targets)

        if acks < self.w:
            raise QuorumNotReached(f"put({key}): only {acks}/{self.w} acks")
        self._log(f"[put] key={key} replicas={targets} acks={acks}/{self.n} (w={self.w})")
        return version

    def _pick_hint_target(self, exclude: set) -> Optional[str]:
        candidates = [nid for nid, n in self.nodes.items() if n.alive and nid not in exclude]
        return random.choice(candidates) if candidates else None

    # ---------- read path ----------
    def get(self, key: str) -> bytes:
        targets = self.meta.get_replicas(key) or self.ring.get_replicas(key, self.n)
        results = []  # (node_id, chunk)
        errors = []

        for target in targets:
            node = self.nodes.get(target)
            if node is None or not node.alive:
                errors.append((target, "unreachable"))
                continue
            try:
                chunk = node.get(key)
                results.append((target, chunk))
            except IntegrityError as e:
                errors.append((target, "corrupt"))
                self._log(f"[corruption-detected] {target} key={key} -> {e}")
                self._repair_corrupt(target, key, targets)
            except KeyError:
                errors.append((target, "missing"))

            if len(results) >= self.r:
                break

        if len(results) < self.r:
            raise QuorumNotReached(f"get({key}): only {len(results)}/{self.r} reads succeeded")

        # pick newest version among what we read; detect + repair stale replicas
        best_node, best_chunk = results[0]
        for nid, chunk in results[1:]:
            cmp = compare_versions(chunk.version, best_chunk.version)
            if cmp == "b":
                best_node, best_chunk = nid, chunk

        # read repair: push newest value to any replica that was behind
        for nid, chunk in results:
            if nid != best_node and compare_versions(chunk.version, best_chunk.version) == "b":
                self.nodes[nid].put(key, best_chunk.data, best_chunk.version)
                self._log(f"[read-repair] patched stale replica {nid} for key={key}")

        return best_chunk.data

    def _repair_corrupt(self, bad_node: str, key: str, targets: List[str]):
        for other in targets:
            if other == bad_node:
                continue
            node = self.nodes.get(other)
            if node and node.alive and node.has(key):
                try:
                    good = node.get(key)
                except IntegrityError:
                    continue
                self.nodes[bad_node].put(key, good.data, good.version)
                self._log(f"[repair] restored {bad_node} key={key} from {other}")
                return

    # ---------- re-replication after permanent node loss ----------
    def _reReplicate_owned_by(self, dead_node: str):
        affected = [k for k in self.meta.all_object_keys() if dead_node in self.meta.get_replicas(k)]
        # prioritize objects furthest below target replication factor
        def deficit(key):
            live = sum(1 for nid in self.meta.get_replicas(key)
                       if nid in self.nodes and self.nodes[nid].alive)
            return self.n - live
        affected.sort(key=deficit, reverse=True)

        for key in affected:
            new_targets = self.ring.get_replicas(key, self.n)
            source = None
            for nid in self.meta.get_replicas(key):
                if nid in self.nodes and self.nodes[nid].alive and self.nodes[nid].has(key):
                    source = nid
                    break
            if source is None:
                self._log(f"[re-replicate][WARN] no healthy source for key={key}, data loss risk")
                continue
            chunk = self.nodes[source].get(key)
            for nid in new_targets:
                if nid != dead_node and not self.nodes[nid].has(key):
                    self.nodes[nid].put(key, chunk.data, chunk.version)
            self.meta.set_replicas(key, new_targets)
            self._log(f"[re-replicate] key={key} rebuilt onto {new_targets}")

    # ---------- rebalancing on membership change ----------
    def _rebalance(self):
        """When the ring changes, some keys' owning node sets change.
        Move affected chunks, throttled to a small budget per call so
        rebalancing never floods foreground traffic."""
        moved = 0
        for key in self.meta.all_object_keys():
            if moved >= self._rebalance_budget_per_tick:
                break
            old_targets = set(self.meta.get_replicas(key))
            new_targets = set(self.ring.get_replicas(key, self.n))
            if old_targets == new_targets:
                continue
            source_id = next((nid for nid in old_targets
                               if nid in self.nodes and self.nodes[nid].alive and self.nodes[nid].has(key)), None)
            if not source_id:
                continue
            chunk = self.nodes[source_id].get(key)
            for nid in new_targets - old_targets:
                self.nodes[nid].put(key, chunk.data, chunk.version)
                moved += 1
            for nid in old_targets - new_targets:
                if nid in self.nodes:
                    self.nodes[nid].delete(key)
            self.meta.set_replicas(key, list(new_targets))
            self._log(f"[rebalance] key={key} moved {old_targets} -> {new_targets}")

    # ---------- background anti-entropy (Merkle diff) ----------
    def anti_entropy_pass(self):
        """Compare Merkle trees across each object's replica set; repair divergence."""
        checked_pairs = set()
        for key in self.meta.all_object_keys():
            targets = [t for t in self.meta.get_replicas(key) if t in self.nodes and self.nodes[t].alive]
            for i in range(len(targets)):
                for j in range(i + 1, len(targets)):
                    a, b = targets[i], targets[j]
                    if (a, b) in checked_pairs:
                        continue
                    checked_pairs.add((a, b))
                    tree_a = self.nodes[a].merkle_tree()
                    tree_b = self.nodes[b].merkle_tree()
                    diverged = tree_a.diverged_keys(tree_b)
                    for k in diverged:
                        self._reconcile_key(k, a, b)

    def _reconcile_key(self, key: str, a: str, b: str):
        node_a, node_b = self.nodes[a], self.nodes[b]
        try:
            chunk_a = node_a.get(key) if node_a.has(key) else None
        except IntegrityError:
            chunk_a = None
        try:
            chunk_b = node_b.get(key) if node_b.has(key) else None
        except IntegrityError:
            chunk_b = None

        if chunk_a and not chunk_b:
            node_b.put(key, chunk_a.data, chunk_a.version)
            self._log(f"[anti-entropy] propagated key={key} {a} -> {b}")
        elif chunk_b and not chunk_a:
            node_a.put(key, chunk_b.data, chunk_b.version)
            self._log(f"[anti-entropy] propagated key={key} {b} -> {a}")
        elif chunk_a and chunk_b:
            cmp = compare_versions(chunk_a.version, chunk_b.version)
            if cmp == "a":
                node_b.put(key, chunk_a.data, chunk_a.version)
                self._log(f"[anti-entropy] {a} newer, patched {b} for key={key}")
            elif cmp == "b":
                node_a.put(key, chunk_b.data, chunk_b.version)
                self._log(f"[anti-entropy] {b} newer, patched {a} for key={key}")

    # ---------- background scrub ----------
    def scrub_all(self):
        for nid, node in self.nodes.items():
            if not node.alive:
                continue
            bad = node.scrub()
            for key in bad:
                self._log(f"[scrub] {nid} found bit-rot on key={key}")
                targets = self.meta.get_replicas(key)
                self._repair_corrupt(nid, key, targets)
