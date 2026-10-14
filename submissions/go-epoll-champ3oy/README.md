# Go (raw epoll) + C hot path

| | |
|---|---|
| Language | Go 1.27.2 (official tarball) and C (gcc from Ubuntu 24.04's `build-essential`) |
| Framework | none: a hand-written epoll event loop and HTTP/1.1 parser on the `syscall` package |
| SQLite | 3.54.0 amalgamation, compiled into the binary through cgo (`build.sh` downloads it and checks its SHA-256) |
| Dependencies | none outside the Go standard library and SQLite |
| **Nginx or direct** | **Direct**: serves `0.0.0.0:80` itself |

## Running it

```bash
sudo bash install.sh   # apt: build-essential, curl, unzip; Go 1.27.2 to /usr/local/go
bash build.sh          # downloads the pinned SQLite amalgamation, go build -> bin/server
SQLITE_PATH=... JWT_SECRET=... HOST=0.0.0.0 PORT=80 bash start.sh
```

## Design, and why

**One epoll loop on one thread, no goroutine per connection.** With 1 vCPU, threads only add context
switches. `net/http` would need a goroutine and two bufio buffers per connection (about 15–20 KB each),
which is close to 1 GB at 60k connections. Here an idle connection is a small struct plus its kernel
socket. All connections share one 64 KB read buffer, and bytes are copied into a connection only when a
request arrives in pieces. The same goes for output: it stays on the connection only when the socket
is full. 60,000 open connections used about 60 MB RSS.

**Direct, not behind Nginx.** Nginx's `worker_connections 16384` caps the Nginx setup at about 16k
users, and Nginx would also take CPU away from the server. The only limit left is the service's
`LimitNOFILE=65535`: one descriptor per user, so about 65k users at most. Past that, new connections
are accepted and closed immediately (a spare descriptor is kept for this), so the listener doesn't spin.

**The SQLite work is in C, with one cgo call per request.** `fastdb.c` binds, steps and resets the
prepared statement, then writes the JSON body straight into the Go output buffer. A feed costs one
Go→C crossing instead of about 120 (a step and five column reads per row with a typical driver).
JSON escaping and integer formatting are done by hand. Go handles HTTP parsing, routing, JWT
verification and request-body validation.

**Group commit.** Likes and new posts are collected while the loop works through one batch of epoll
events. They run in a single `BEGIN IMMEDIATE … COMMIT`, and only after the commit are their
201/200 responses sent (rule 6). Reads in the same batch are answered right away.

**Checkpoints off the event loop.** `wal_autocheckpoint` is off. After a commit pushes the WAL past
1,000 frames (SQLite's default threshold), a second thread runs a PASSIVE checkpoint on its own
connection, and a pipe in the epoll set signals when it finishes. Two dead ends led here:

- With an inline checkpoint, its fsyncs stalled every request for 35–60 ms each time.
- With a free-running background checkpointer, the WAL grew without bound (300 MB in 3 minutes). SQLite
  only rewinds the WAL once a checkpoint has caught up with every frame, and a checkpoint racing a
  writer never catches up.

So reads keep flowing during a checkpoint, but the next group commit waits a few milliseconds for it
to finish. The checkpoint always catches up, and the WAL stays around 4 MB.

**Pragmas**: `journal_mode=WAL`, `synchronous=NORMAL` (rule 6), 1 GiB `mmap_size`, 32 MiB page cache,
`temp_store=MEMORY`.

**SQLite compile options**: `THREADSAFE=2` (each connection belongs to one thread), `DEFAULT_MEMSTATUS=0`
(no global lock on every malloc), `OMIT_DEPRECATED`, `OMIT_SHARED_CACHE`, `OMIT_PROGRESS_CALLBACK`,
`OMIT_DECLTYPE`, `OMIT_LOAD_EXTENSION`, `USE_ALLOCA`, `DQS=0`, `MAX_EXPR_DEPTH=0`. These come from
SQLite's list of recommended compile-time options.

**JWT**: HS256 only. The signature is checked on every request with `crypto/hmac` (constant-time
compare); the keyed HMAC state is reused through `Reset()`, but nothing about tokens is remembered
(rule 5). The common header `{"alg":"HS256","typ":"JWT"}` is matched as bytes, and any other header is
parsed as JSON. `exp`/`nbf` are checked.

**SQL**: the reference queries. A like is a single `INSERT … SELECT … WHERE EXISTS (post) ON CONFLICT DO
NOTHING`; the post-existence check runs only when nothing was inserted.

**Go runtime**: `GOGC=400`. Reads allocate nothing per request (the C code writes into a reused
buffer), so collections are rare and small.

## Measured locally

Measured in Docker on an Apple-silicon Mac. The server was pinned to one core with 2 GB of memory and
`nofile=65535`. A load generator that copies `bench/load.js` user behaviour (one keep-alive connection
per user, same think times and like/post probabilities) ran in a separate container, with a 60 s ramp
and a 2–3 minute hold:

| Users | req/s | p95 | p99 | errors | server CPU (one core) |
|---|---|---|---|---|---|
| 5,000 | 530 | 1 ms | 4 ms | 0 | ~11% |
| 20,000 | 2,110 | 1 ms | 6 ms | 0 | ~20–35% |
| 60,000 | 6,350 | 4–11 ms | 129–235 ms | 0 | ~25–40% |

One of four 60k runs failed with multi-second stalls. Logging inside the loop showed even non-blocking
socket writes taking 20–70 ms, so the server thread was being descheduled by the busy host, not
blocked. A single core on this Mac is faster than a droplet vCPU, and kernel network work partly ran
on other cores here, so the droplet will be slower. Even so, CPU was far from the limit. The limit
that binds is the descriptor ceiling at about 65k connections.

## License

MIT, under the repo's [license](../../LICENSE).
