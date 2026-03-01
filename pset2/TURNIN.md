Problem set 2 turnin
====================

## Changes to `netsim.hh`

1. **Jitter model**: I experimented with using Exponential and Gaussian jitters added ontop of a base delay. For the final failure, I used gaussian and for the others I used exponential. 

2. **`recv_delay_`**: Added a configurable receive-side computation delay (e.g., per-message CPU processing time on the receiving server)

## Adding failure support

I added crash-stop failure support across both `netsim.hh` and `ctconsensus.cc`:

### `netsim.hh`
- `channel::fail()` / `channel::failed()`: Added a `failed_` boolean to `channel<T>`. Calling `fail()` marks the channel as permanently dead; all future `send()` calls on a failed channel silently drop the message.

- `fail_server_after(net, i, N, delay)`: A coroutine helper that sleeps for `delay`, then calls `fail()` on every outgoing channel from server `i` (including the link to Nancy at id −1).

### `ctconsensus.cc`

- `-f` / `--failures` flag: Controls how many servers crash. `-f 0` = no crashes, `-f 2` = exactly 2 crashes, and the default (`-f -1`) picks a random number of failures in `[0, (N−1)/2]` — always staying within the CT tolerance of `< N/2` crashes.
- 
## Bug flags

#### `--weakfd` / `-w`: Weakened failure detector

Sets the FD timeout to **66ms** instead of 1500ms to get a 10% message failure based on the Exponential distribution parameter I used for this bug. This violates eventual weak accuracy. No correct server can avoid permanent suspicion across enough rounds, preventing the algorithm from terminating.

**Type of failure**: Liveness (Nancy gets impatient).

**Reproducing**:
```
build/ctconsensus --weakfd -n 5 -f 1 -S 5322027244979987980
```

#### `--dropdecide` / `-d`: Drop DECIDE retransmissions

When a server decides:
- If it decided **as leader** (Phase 5, got >N/2 ACKs): sends DECIDE to Nancy, then to only ~half of other servers (each independently dropped with 50%
  probability).
- If it decided by receiving DECIDE from another server: sends DECIDE to Nancy only, skips rebroadcast entirely.

This differs from the rebroadcasting of DECIDE messages. With server crashes, some alive servers never learn the decision and stay stuck in the round loop. Nancy doesn't receive enough (>N/2) DECIDEs.

**Type of failure**: Liveness (Nancy gets impatient).

**Reproducing**:
```
build/ctconsensus -n 5 -S 10362806899721272284 --dropdecide
```

#### `--skipround` / `-s`: Skip `color_round_` update in Phase 4

When a server receives PROPOSE in Phase 4, it updates its color but skips `color_round_ = round_`. Without this update, the server's subsequent PREPARE messages carry a stale (too-low) `color_round_`. A future leader sees the stale timestamp and may override a color that a majority already locked in, causing two servers to decide on different colors. With this, as per Ed Post 47, we reduce the FD timeout to 100ms and also change our jitter distribution to normal.

**Type of failure**: Safety (consensus violation: two different colors decided).

**Reproducing**:
```
build/ctconsensus -n 7 -f 0 -s -S 17642637639949184403
```
