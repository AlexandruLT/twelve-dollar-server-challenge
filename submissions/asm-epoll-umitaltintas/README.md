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
- **Small memory footprint.** Each connection is a 32-byte record indexed by fd. Buffers are
  allocated only for a partial request or for output the socket could not take. The server's RSS
  stays around 25 MB with 64k open connections.

## Results (local, not the benchmark box)

Measured on an Apple Silicon Mac. The server ran in an `linux/amd64` Ubuntu 24.04 container
limited to `--cpus=1 --memory=2g`, so it was **Rosetta-emulated**. The load came from a Go
reimplementation of `bench/load.js` (same user loop, think times and probabilities), because k6
itself became the bottleneck at around 15k VUs on this machine. Both ran on the same machine,
with no real network in between. The numbers are only useful for comparing changes to this code;
they are not a prediction for the droplet.

| VUs (2 min hold) | p50 | p95 | p99 | failed | server CPU | server RSS |
|---|---|---|---|---|---|---|
| 20,000 | 0.7 ms | 2.3 ms | 5.5 ms | 0% | ~30% | ~40 MB |
| 48,000 | 0.9 ms | 3.6 ms | 24 ms | 0% | ~46% | ~25 MB |
| 64,000 | 0.6 ms | 3.5 ms | 38 ms | 0% | ~42% | ~23 MB |

`LimitNOFILE=65535` caps the server at about 65.5k connections, so 64k VUs is the ceiling for any
submission.

## Trade-offs

This is a server specialised for this API. It implements only the parts of HTTP/1.1 the API
needs: no chunked request bodies and no `Expect: 100-continue`, and request bodies over 32 KB are
rejected with 400. It runs only on x86-64 Linux. `synchronous=NORMAL` means a committed write can
be lost on power failure, though not on a process crash. That is the trade-off rule 6 allows.
