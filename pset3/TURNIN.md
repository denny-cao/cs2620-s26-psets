Notebook
---
# Original Attempt: BUG Commit 6582514 
While working on Phase 3, I reached this error:
```bash
build/pt-paxos -l 0.01 -R 100 2>&1
*** FAILURE on seed 14217800381867498222 at replica 1 key g19/v031
...
g19/v026 [V664] c18 ebf5cd2
g19/v027 [V684] c18 ebf5cd2
g19/v028 [V705] c18 ebf5cd2
g19/v029 [V729] c18 ebf5cd2
g19/v030 [V752] c18 ebf5cd2
g25/lock [V38] c0 0eb6892
g25/v000 [V62] c0 0eb6892
g25/v001 [V86] c0 0eb6892
g25/v002 [V111] c0 0eb6892
g25/v003 [V134] c0 0eb6892
g25/v004 [V158] c0 0eb6892
...
```

The gap is between g19/v030 and g25/lock. Replica 1 is missing g19/v031 and everything after it up to some point. I believed this to be a catch-up gap after election. Looking around the trace, this is something I found that could be the reason:

```bash
build/pt-paxos -l 0.01 -V -S 14217800381867498222 2>&1 | grep -E "PROBE|PREPARE|PROPOSE|DECIDE" | grep "20:21:2[4-9]\|20:21:3" | head -40
2021-10-12 20:21:24.005001121: R2 → R0/r PROBE(round=2474, from=R2)
2021-10-12 20:21:24.006001121: R2 → R1/r PROBE(round=2474, from=R2)
2021-10-12 20:21:24.007001121: R2 → R2/r PROBE(round=2474, from=R2)
2021-10-12 20:21:24.008001122: R1 → R0/r PROBE(round=2401, from=R1)
2021-10-12 20:21:24.009001122: R1 → R1/r PROBE(round=2401, from=R1)
2021-10-12 20:21:24.010001122: R1 → R2/r PROBE(round=2401, from=R1)
2021-10-12 20:21:24.011001122: R1/r ← R2 PROBE(round=2474, from=R2)
2021-10-12 20:21:24.011001122: R1 → R2/r PREPARE(round=2474, from=R1)
2021-10-12 20:21:24.011001124: R0/r ← R2 PROBE(round=2474, from=R2)
2021-10-12 20:21:24.013001125: R2/r ← R2 PROBE(round=2474, from=R2)
2021-10-12 20:21:24.013001125: R2 → R2/r PREPARE(round=2474, from=R2)
2021-10-12 20:21:24.014001126: R0/r ← R1 PROBE(round=2401, from=R1)
2021-10-12 20:21:24.015001125: R1/r ← R1 PROBE(round=2401, from=R1)
2021-10-12 20:21:24.016001125: R2/r ← R1 PROBE(round=2401, from=R1)
2021-10-12 20:21:24.016001125: R2/r ← R1 PREPARE(round=2474, from=R1)
2021-10-12 20:21:24.019001127: R2/r ← R2 PREPARE(round=2474, from=R2)
2021-10-12 20:21:24.845001120: R2 → R0/r PROPOSE(slot=825, serial=139265)
2021-10-12 20:21:24.846001120: R2 → R1/r PROPOSE(slot=825, serial=139265)
2021-10-12 20:21:24.847001120: R2 → R2/r PROPOSE(slot=825, serial=139265)
2021-10-12 20:21:24.852001089: R0/r ← R2 PROPOSE(slot=825, serial=139265)
2021-10-12 20:21:24.852001123: R1/r ← R2 PROPOSE(slot=825, serial=139265)
2021-10-12 20:21:24.853001124: R2/r ← R2 PROPOSE(slot=825, serial=139265)
2021-10-12 20:21:24.858001126: R2 → R0/r DECIDE(slot=825, serial=139265)
2021-10-12 20:21:24.859001126: R2 → R1/r DECIDE(slot=825, serial=139265)
2021-10-12 20:21:24.860001126: R2 → R2/r DECIDE(slot=825, serial=139265)
2021-10-12 20:21:24.864001129: R0/r ← R2 DECIDE(slot=825, serial=139265)
2021-10-12 20:21:24.865001129: R1/r ← R2 DECIDE(slot=825, serial=139265)
2021-10-12 20:21:24.866001128: R2/r ← R2 DECIDE(slot=825, serial=139265)
2021-10-12 20:21:24.899001103: R2 → R1/r PROPOSE(slot=826, serial=131083)
2021-10-12 20:21:24.900001103: R2 → R2/r PROPOSE(slot=826, serial=131083)
2021-10-12 20:21:24.905001106: R1/r ← R2 PROPOSE(slot=826, serial=131083)
2021-10-12 20:21:24.906001107: R2/r ← R2 PROPOSE(slot=826, serial=131083)
2021-10-12 20:21:24.911001109: R2 → R0/r DECIDE(slot=826, serial=131083)
2021-10-12 20:21:24.912001109: R2 → R1/r DECIDE(slot=826, serial=131083)
2021-10-12 20:21:24.913001109: R2 → R2/r DECIDE(slot=826, serial=131083)
2021-10-12 20:21:24.917001112: R0/r ← R2 DECIDE(slot=826, serial=131083)
2021-10-12 20:21:24.918001112: R1/r ← R2 DECIDE(slot=826, serial=131083)
2021-10-12 20:21:24.919001111: R2/r ← R2 DECIDE(slot=826, serial=131083)
2021-10-12 20:21:25.007001120: R2 → R0/r PROPOSE(slot=827, serial=143387)
2021-10-12 20:21:25.008001121: R2 → R1/r PROPOSE(slot=827, serial=143387)
```

R2 wins election and jumps straight to slot 825 but R1 is missing g19/v031 which is a much earlier slot. R2 never sent catch-up DECIDEs for the gap between R1's `next_decide_slot_` and 825. So the new leader learns only a length, not the actual missing contents for the prefix. If one follower is behind at slot 31 and another is at 824, the new leader skips straight to 825.

The leader needs actual log/value data, not just lengths!! I asked ChatGPT to compare my implementation attempt vs. class notes and RAFT, and ChatGPT instructed me to change PREPARE to include acknowledged value information => leader chooses a sequence/value based on that information. I used Copilot to attempt to fix this, but I just kept running into more and more issues. My entire protocol was not maintaining or reasoning about accepted state correctly. Leader's proposal logic was based on maximum slot instead of correct val seq from the quorum, replicas weren't correctly ignoring stale rounds (Since I did not add in round info past Phase 1), same round PROPOSALs could result in overwrites instead of extensions, etc etc etc. 

I opted to start over to try to understand Mulit-Paxos better.

# Phase 1
Under the assumption of failure-free, stable-leaders, we want non-leaders to keep databases up to date without a need to consider recovery/selection of a new leader. We have the leader extend the log one request at a time and followers then accept that value sequence. Once a majority has the current prefix, it is decided.

# Phase 2/3 BUG
I opted to change from Phase 1 to map to the Multi-Paxos notes without truncation. We add PROBE and PREPARE messages to relax the assumption of a fixed leader. We also add a timeout to handle retransmissions and new leader selection. The key change from my previous attempt of Phase 3 is now adding AV in PREPAREs. 

To implement truncation, we move from slot-by-slot requests to suffixes where we mark start/ends of suffixes of ACKs and DECIDEs so we can drop sequences where all have agreed that were already DECIDEd. However, I ran into bug at Commit 3c1b517.

```
*** FAILURE on seed 14102059275915900385 at replica 1 key g19/lock
...
g13/v000 [V39] c13 ebdf502
g13/v001 [V47] c13 ebdf502
g13/v002 [V63] c13 ebdf502
g13/v003 [V66] c13 ebdf502
g17/lock [V17] c24 fd79093
g20/lock [V18] c29 89000d8
g20/v000 [V21] c29 89000d8
g20/v001 [V34] c29 89000d8
g20/v002 [V36] c29 89000d8
g20/v003 [V37] c29 89000d8
g20/v004 [V41] c29 89000d8
...
```
Truncation was too agressive, deleting prefix state based on local truth instead of global safe truncation. I had local truncation in handing probe and propose:

- `handle_probe`: `local_truncate(m.dv)`. Only says leader has decided this much but does not imply every replica has applied that prefix so we cannot guarentee a safe truncation here.
- `handle_propose`: `local_truncate(decided_len_)`. Only says that locally applied up to that amount but does not say that it is a safe garbage-collect everywhere. 

I was still running into errors though.

# Phase 4: BUG Commit 3c1b517

Dedup implemented to cache. If the client retries exact same request, the leader can answer immediately. But I still was running into issues. I pasted my code into ChatGPT and it told me "proposal construction is using the wrong abstraction." I used a queue of incoming requests for pending client requests, but they need to be an absolute-slot log suffix. I let ChatGPT reimplement this. It replaced my `IVs_` and `iv_base_` logic with a `pending_reqs_` queue and helper functions that appended pending requests onto the best accepted value from PREPAREs. The reasoning was that my old code was conflating "pending client work" with "accepted log state," which only happened to work with no loss and broke badly under loss and leader changes.

Another issue ChatGPT found was with client replies. Originally, whichever replica currently thought it was leader would send responses when applying decisions. That was wrong, because leadership at apply time does not mean that replica is the one responsible for replying to that client request. ChatGPT changed this by tracking `waiting_client_reqs_`, so a replica only sends a response for a request it actually received and is still waiting to answer. That made replies line up with request ownership instead of current leader identity.

# Phase 4: Cleanup retransmissions
Observing the traces, I noticed a lot of PROPOSE and DECIDE retransmissions that maybe could be better optimized. ChatGPT suggested refactoring `handle_timeout()` to, instead of having a retransmission after every time, only doing so when needed. ChatGPT made these changes: Instead of always broadcasting, the leader now checks two conditions:

- whether there is an undecided suffix (i.e., the current proposal extends beyond the decided prefix), and
- whether any replica is behind in applied state (using `applied_q`).

PROPOSE messages are retransmitted if there is still an undecided suffix or if replicas may be behind, ensuring that lagging replicas can still learn the current value sequence. DECIDE messages are retransmitted only when replicas are behind, since they are used to advance applied state and trigger safe truncation.
