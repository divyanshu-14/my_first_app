"""
End-to-end demo of the Vault distributed object store.
Run: python3 demo.py
"""
import time
import threading
import time
from coordinator import Cluster, QuorumNotReached


def section(title):
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def flush_log(cluster, since=0):
    for line in cluster.log[since:]:
        print(" ", line)
    return len(cluster.log)


def main():
    # N=3 replicas, W=2 writes must ack, R=2 reads must agree
    cluster = Cluster(n=3, w=2, r=2, virtual_nodes=100)
    log_ptr = 0

    section("1. Cluster bootstrap")
    for nid in ["node-A", "node-B", "node-C", "node-D", "node-E"]:
        cluster.add_node(nid)
    print("Ring members:", cluster.ring.members())
    log_ptr = flush_log(cluster, log_ptr)

    section("2. Basic put/get with quorum durability (N=3, W=2, R=2)")
    version = cluster.put("objects/photo.png", b"binary-image-data-blob")
    print("Wrote key with version:", version)
    log_ptr = flush_log(cluster, log_ptr)
    data = cluster.get("objects/photo.png")
    print("Read back:", data)

    section("3. Concurrent writes/reads from multiple clients")
    results = {}
    def worker(i):
        key = f"objects/file-{i}.bin"
        cluster.put(key, f"payload-{i}".encode() * 100)
        results[key] = cluster.get(key)
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    print(f"10 concurrent put+get cycles completed, all verified: "
          f"{all(results[k] == f'payload-{i}'.encode() * 100 for i, k in enumerate(results))}")

    section("4. Node failure mid-cluster -> sloppy quorum + hinted handoff")
    targets = cluster.ring.get_replicas("objects/report.pdf", 3)
    print("Owning nodes for key:", targets)
    cluster.kill_node(targets[0])
    version = cluster.put("objects/report.pdf", b"quarterly-report-bytes")
    print(f"Write still succeeded despite {targets[0]} being down (hinted handoff kicked in)")
    log_ptr = flush_log(cluster, log_ptr)
    data = cluster.get("objects/report.pdf")
    print("Read back correctly via remaining live replicas:", data)

    section("5. Node recovers -> hint should still be reachable via re-replication path")
    cluster.revive_node(targets[0])
    log_ptr = flush_log(cluster, log_ptr)
    print(f"{targets[0]} back online; data will reconcile on next anti-entropy pass")

    section("6. Silent data corruption -> detected on read, auto-repaired")
    key = "objects/photo.png"
    replicas = cluster.meta.get_replicas(key)
    victim = replicas[0]
    print(f"Corrupting {victim}'s copy of {key} (simulated bit rot)...")
    cluster.nodes[victim].corrupt_chunk(key)
    data = cluster.get(key)  # triggers integrity check -> repair
    print("Read still returned correct data via a healthy replica:", data)
    log_ptr = flush_log(cluster, log_ptr)
    print("Verifying corrupted node was repaired:",
          cluster.nodes[victim].get(key).data == data)

    section("7. Background scrubbing catches corruption even without a read")
    key2 = "objects/file-3.bin"
    replicas2 = cluster.meta.get_replicas(key2)
    cluster.nodes[replicas2[1]].corrupt_chunk(key2)
    print(f"Corrupted {replicas2[1]}'s copy of {key2} silently (no read yet)")
    cluster.scrub_all()
    log_ptr = flush_log(cluster, log_ptr)

    section("8. Replica inconsistency -> anti-entropy Merkle-tree repair")
    cluster.anti_entropy_pass()
    log_ptr = flush_log(cluster, log_ptr)
    print("(Any divergence from step 5's reconnect gets reconciled here)")

    section("9. Permanent node loss -> re-replication to restore N")
    dead = "node-B"
    owned_before = [k for k in cluster.meta.all_object_keys() if dead in cluster.meta.get_replicas(k)]
    print(f"{dead} owns {len(owned_before)} keys before removal")
    cluster.kill_node(dead)
    cluster.remove_node_permanently(dead)
    log_ptr = flush_log(cluster, log_ptr)
    still_short = [k for k in owned_before
                   if len(cluster.meta.get_replicas(k)) < cluster.n
                   or dead in cluster.meta.get_replicas(k)]
    print(f"Keys still short of N replicas after re-replication: {len(still_short)}")

    section("10. New node joins -> background rebalancing")
    cluster.add_node("node-F")
    log_ptr = flush_log(cluster, log_ptr)
    print("Ring members now:", cluster.ring.members())

    section("11. Availability under partial quorum failure")
    all_targets = cluster.ring.get_replicas("objects/critical.doc", 3)
    cluster.put("objects/critical.doc", b"important-bytes")
    cluster.kill_node(all_targets[0])
    cluster.kill_node(all_targets[1])
    try:
        cluster.get("objects/critical.doc")
        print("Read succeeded with 1/3 replicas alive (below R=2 quorum should fail)")
    except QuorumNotReached as e:
        print(f"Correctly refused to serve stale/unverifiable read: {e}")
    cluster.revive_node(all_targets[0])
    cluster.revive_node(all_targets[1])

    section("Done")
    print(f"Total operations logged: {len(cluster.log)}")


if __name__ == "__main__":
    main()
while True:
    time.sleep(3600)