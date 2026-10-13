# Java + its own NIO event loop

| | |
|---|---|
| Language | Java 25: Eclipse Temurin `jdk-25.0.4.1+1`, official tarball, sha256-pinned in `install.sh` |
| Framework | None. A hand-written HTTP/1.1 server on `java.nio` (one `Selector`, epoll on Linux) |
| SQLite driver | No JDBC. Direct FFM (`java.lang.foreign`) bindings to SQLite **3.54.0**, compiled from the amalgamation by `build.sh` (sha256-pinned) |
| JSON | Responses are built by SQLite (`json_object` / `json_group_array`); a small strict parser reads request bodies and JWTs |
| Dependencies | Zero Java dependencies. `build.sh` needs only `cc` and the JDK |
| **Nginx or direct** | **Direct**: serves `0.0.0.0:80` itself |

## Running it

```bash
sudo bash install.sh   # apt: build-essential, curl; Temurin 25 to /opt/jdk
bash build.sh          # SQLite -> build/libsqlite3.so, sources -> build/server.jar
SQLITE_PATH=... JWT_SECRET=... HOST=0.0.0.0 PORT=80 bash start.sh
```

## Optimizations, and why

**Process and I/O**
- **One thread, one SQLite connection, everything inline.** On one vCPU, more threads only add context
  switches. Every query is an index lookup that takes microseconds, so the event loop calls SQLite
  directly.
- **No buffers while idle.** Each connection is a small object. One shared direct read buffer and one shared
  response buffer serve every connection. A connection gets its own buffer only while it holds a partial
  request or a response the socket didn't take yet. 60,000 open connections cost about 40 MB of heap.
- **Direct, no Nginx.** Nginx would take CPU from the one core, and `worker_connections 16384` caps it at
  about 16k users.
- **Past the descriptor limit: idle eviction.** Connections sit in an LRU list. Near the `LimitNOFILE` limit,
  the server closes the longest-idle connections that have nothing in flight, so `accept` never fails with
  `EMFILE`. A client that was evicted while idle just reconnects on its next request.
- **Adaptive wake-up batching.** When a wake-up brought fewer than 32 events, the loop parks for 2 ms so
  the next `epoll_wait` returns many at once. With ~0.1 req/s per user, most wake-ups would otherwise
  carry a single request. This saves about 10% of CPU for about 2 ms of latency. Busy wake-ups don't
  park, so the pause never caps throughput.
- **Explicit accept backlog** of 4096 (the JDK default is 50).

**SQLite**
- **JSON built in SQL.** Each read returns the whole response body as one TEXT column. A feed costs one
  `sqlite3_step`, one `column_text` and one native-to-native `memcpy` into the response buffer: no Java
  strings, no per-column calls, no escaping in Java.
- **FFM instead of JDBC.** Statements are prepared once (`SQLITE_PREPARE_PERSISTENT`) and reused.
  Short calls (bind, column, changes) are `Linker.Option.critical` downcalls.
- **`SQLITE_ENABLE_QPSG`.** Without it, the `GET /posts/:id` statement (`p.id = ?1` plus a correlated count)
  was re-prepared on every bind, because SQLite thought the bound value might change the plan.
- **Group commit.** Likes and new posts are queued during a wake-up and run together in one
  `BEGIN IMMEDIATE … COMMIT`. Their responses are written only after `COMMIT` returns (rule 6). Requests
  pipelined behind a write are answered after it, in order.
- **Checkpoints on our schedule.** `wal_autocheckpoint=0`. A PASSIVE checkpoint
  (`sqlite3_wal_checkpoint_v2`) runs between batches about once a second, never inside a request's
  commit.
- **Pragmas:** `locking_mode=EXCLUSIVE` (no shared-memory WAL index and no file locking per transaction),
  `journal_mode=WAL`, `synchronous=NORMAL` (rule 6), `cache_size=-65536`, `temp_store=MEMORY`,
  `journal_size_limit=64MB`.
- **Compile options:** `-O2 -march=native` with `THREADSAFE=0`, `DEFAULT_MEMSTATUS=0`,
  `DEFAULT_WAL_SYNCHRONOUS=1`, `DQS=0`, `LIKE_DOESNT_MATCH_BLOBS`, `MAX_EXPR_DEPTH=0`, `OMIT_DECLTYPE`,
  `OMIT_DEPRECATED`, `OMIT_PROGRESS_CALLBACK`, `OMIT_SHARED_CACHE`, `OMIT_LOAD_EXTENSION`, `USE_ALLOCA`,
  `ENABLE_QPSG`.

**Requests**
- **HTTP parsing on raw bytes.** It reads only the request line, `Content-Length`, `Authorization` and
  `Connection`. Status lines and headers are precomputed, and each response goes out in one `write`.
- **JWT verified in full on every request** (rule 5). One keyed `javax.crypto.Mac` is reused, the
  signature is base64url-encoded straight from the digest and compared in constant time, and the token
  must be HS256 with valid `exp`/`nbf`.

**JVM**
- `-XX:+UseSerialGC` (no GC threads competing for the core), a fixed 256 MB heap, `-XX:+AlwaysPreTouch`,
  `-XX:+UseCompactObjectHeaders`, and capped metaspace, code cache and direct memory. Steady state
  allocates about 1 MB/s, so a young GC happens every couple of minutes.

## Local measurements

These were measured on Docker on an Apple Silicon laptop, not the droplet. The server container was
limited to `--cpus=1 --memory=2g` with `nofile=65535`. Load came from separate containers replaying
`bench/load.js`'s user loop with one keep-alive connection per user.

| Users | req/s | p95 | p99 | Errors | Server CPU |
|---|---|---|---|---|---|
| 10,000 (k6) | ~1,050 | 3.5 ms | 5.1 ms | 0 | ~8% |
| 60,000 | ~6,350 | 4.5 ms | 15 ms | 0 | ~30% |
| 80,000 (eviction active) | ~8,470 | 15 ms | 24 ms | 0.03% | ~33% |

## License

MIT, under the repo's [license](../../LICENSE).
