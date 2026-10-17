# x86-64 assembly (NASM) + epoll + SQLite

The whole server is hand-written x86-64 assembly: event loop, HTTP/1.1 parser, router, JSON
reader/writer, base64url, SHA-256/HMAC and JWT verification. SQLite is the only C code in the
binary, as the rules require. It is built from the pinned amalgamation and called from assembly
through the System V ABI. libc is linked only because SQLite needs it. All socket I/O uses raw
syscalls.

| | |
|---|---|
| Language | x86-64 assembly, NASM 2.16 (Ubuntu 24.04 package) |
| Framework | none: one epoll loop, raw syscalls |
| SQLite | 3.50.4 amalgamation, downloaded by `build.sh` and checked against its sha256 |
| C compiler | gcc (only for `sqlite3.c`) |
| Nginx or direct | **Direct**: serves `HOST:PORT` itself (`0.0.0.0:80` on the benchmark box) |

About 3,200 lines in `src/`: `server.asm` (loop, HTTP, handlers, auth), `json.inc`,
`sha256.inc` and `defs.inc`.

## How it works

One thread runs an epoll loop over one SQLite connection. Each `epoll_wait` batch goes through
four steps:

1. Every ready socket is read once (level-triggered) and all complete requests in the buffer are
   parsed and handled. Partial requests are kept per connection until the rest arrives.
2. The first write of the batch (`POST /posts` or a like) opens a transaction. Every later write in
   the batch joins it. This is **group commit**.
3. Each response's JSON body is built in a scratch buffer, then copied with its status line and
   headers into one batch arena.
4. At the end of the batch the transaction is **committed first**, and only then are all responses
   sent. A 201 therefore always means the row is committed, and no read in the batch can show a
   write that might still roll back. If a commit fails, the connections waiting on it are closed
   instead of being answered.

## Optimizations, and why

- **No Nginx, no framework.** A request costs about 4 µs of server CPU on `/health`, so the
  per-request overhead is in the kernel's TCP stack and in SQLite, not in this code.
- **Responses come out of SQLite without intermediate objects.** Column text is escaped directly
  into the output buffer (`w_jstr`), with no row objects or serializer in between.
- **Prepared statements are created once and reused** (`SQLITE_PREPARE_PERSISTENT`). Like counts
  come from a correlated `count(*)` on the covering `likes_post_id_idx`. I also measured a
  one-range-scan variant (a CTE plus `BETWEEN min AND max`); it was about 30% slower, so the
  simple query stayed.
- **JWT verification without allocation.** The HMAC inner and outer states are computed once from
  the secret at startup, so each token costs 3–4 SHA-256 compressions. Every token is still
  verified on every request. The header (`alg` must be `HS256`) and the payload are parsed with a
  strict RFC 8259 reader.
- **Checkpoints are kept off the request path, and the WAL is kept small.** This one mattered
  most. `wal_autocheckpoint` is off. A background thread with its own connection runs `PASSIVE`
  checkpoints, which does the page copying and the fsync. Under constant writes a background
  checkpoint never quite reaches the end of the WAL, and SQLite only rewinds the WAL after a
  complete checkpoint. Without the next step the WAL grew to 247 MB in 90 seconds, and `/feed`
  slowed from 37 µs to 123 µs, because every page lookup probes every WAL hash segment. So once the
  WAL reaches 4096 frames (16 MB), the event loop finishes the checkpoint itself right after
  sending a batch's responses. Nothing is writing at that moment, so the checkpoint completes and
  the next commit starts the WAL from the beginning. `journal_size_limit` truncates the file.
- **Pragmas** (all allowed by rule 6): `journal_mode=WAL`, `synchronous=NORMAL`,
  `mmap_size=1 GiB` (reads come from the OS page cache), and a small 8 MB SQLite page cache.
- **Keep-alive is capped below the fd limit, so connections beyond 65k still get served.** The
  service runs with `LimitNOFILE=65535`. That is one fd per open connection, and keep-alive
  clients that are thinking for 3–15 s hold theirs the whole time. Without a cap, the
  65,536th client could not connect at all. Once 60,000 connections are open, newly accepted ones
  are answered with `Connection: close` (one request each), while the existing keep-alive
  connections stay as they are. Deciding per connection matters: when I tried it per request, the
  open count hovered at the cap, nearly every response closed its connection, and the churn
  doubled. The server closes first, so `TIME_WAIT` stays on the server, where it costs no fd and
  no client port. A short connection costs about 10 µs of extra kernel time. If the fd table fills
  up anyway, the listen socket is taken out of epoll until a connection closes, so the loop
  doesn't spin on `EMFILE`; new clients wait in the kernel's accept queue meanwhile.
- **Small memory footprint.** Each connection is a 32-byte record indexed by fd. Buffers are
  allocated only for a partial request or for output the socket could not take. The server's RSS
  stays around 25 MB with 64k open connections.

## Results (local, not the benchmark box)

Measured on an Apple Silicon Mac. The server ran in a `linux/amd64` Ubuntu 24.04 container
pinned to one core (`--cpuset-cpus=0 --memory=2g`, `nofile=65535`), so it was
**Rosetta-emulated**. The load came from a Go reimplementation of `bench/load.js` (same user loop,
think times and probabilities, a new connection after `Connection: close`), split over four
containers on the other cores, because k6 itself became the bottleneck at around 15k VUs on this
machine. There was no real network in between, and the kernel's work for the load generators'
packets ran on their cores, not the server's. The numbers are only useful for comparing changes
to this code; they are not a prediction for the droplet.

| VUs (90 s ramp, 5 min hold) | p50 | p95 | p99 | failed | result |
|---|---|---|---|---|---|
| 88,000 | 0.7 ms | 43 ms | 290 ms | 0% | pass |
| 96,000 | 1.3 ms | 520–590 ms | 1.9 s | 0% | fail (CPU) |

At 88k, 60k clients stay on keep-alive connections and the other 28k open a new connection per
request. The server's RSS stays around 23 MB. At 96k the single core runs out: there are no failed
requests, but queueing pushes p99 past 1 s. Before the keep-alive cap, everything above about 64k
VUs failed outright, because the 65,536th connection could not be accepted.

Taking new connections every 128 events within a batch (not just once per `epoll_wait`) mattered
near the limit. At 96k it cut the kernel's accept-queue overflows from 49,442 to 2,840, and SYN
retransmissions from 565 to 0. Each dropped SYN costs the client at least 1 s.

## Trade-offs

This is a server specialised for this API. It implements only the parts of HTTP/1.1 the API
needs: no chunked request bodies and no `Expect: 100-continue`, and request bodies over 32 KB are
rejected with 400. It runs only on x86-64 Linux. `synchronous=NORMAL` means a committed write can
be lost on power failure, though not on a process crash. That is the trade-off rule 6 allows.
