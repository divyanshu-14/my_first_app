"""
Simplified phi-accrual failure detector. Instead of "no heartbeat for
X seconds = dead" (which false-positives on GC pauses / slow disks and
under-reacts to genuinely fast failures), it tracks the recent inter-arrival
time distribution and computes a continuous suspicion level (phi). Callers
threshold on phi rather than on a raw timeout.
"""

import math
import time
from collections import deque
from typing import Dict


class PhiAccrualDetector:
    def __init__(self, window: int = 20, phi_threshold: float = 8.0):
        self.window = window
        self.phi_threshold = phi_threshold
        self._intervals: Dict[str, deque] = {}
        self._last_heartbeat: Dict[str, float] = {}

    def heartbeat(self, node_id: str, now: float = None):
        now = now if now is not None else time.time()
        if node_id in self._last_heartbeat:
            interval = now - self._last_heartbeat[node_id]
            hist = self._intervals.setdefault(node_id, deque(maxlen=self.window))
            hist.append(interval)
        self._last_heartbeat[node_id] = now

    def phi(self, node_id: str, now: float = None) -> float:
        now = now if now is not None else time.time()
        if node_id not in self._last_heartbeat:
            return 0.0
        hist = self._intervals.get(node_id)
        elapsed = now - self._last_heartbeat[node_id]
        if not hist or len(hist) < 2:
            # not enough history -- fall back to a conservative fixed window
            return 0.0 if elapsed < 2.0 else 10.0
        mean = sum(hist) / len(hist)
        var = sum((x - mean) ** 2 for x in hist) / len(hist)
        std = math.sqrt(var) or 0.001
        # probability elapsed time is drawn from the observed normal dist
        y = (elapsed - mean) / std
        p_later = 0.5 * math.erfc(y / math.sqrt(2))
        p_later = max(p_later, 1e-10)
        return -math.log10(p_later)

    def is_suspected(self, node_id: str, now: float = None) -> bool:
        return self.phi(node_id, now) >= self.phi_threshold
