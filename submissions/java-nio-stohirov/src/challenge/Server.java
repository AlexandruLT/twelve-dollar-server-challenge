package challenge;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;

final class Server {
    private static final int READ_CAP = 64 * 1024;
    private static final int MAX_HEAD = 16 * 1024;
    private static final int MAX_BODY = 32 * 1024;
    private static final int OUT_CAP = 1 << 20;
    private static final int OUT_RESERVE = 64 * 1024;
    private static final long CHECKPOINT_NANOS = 1_000_000_000L;

    private static final int NOT_FOUND = 0, HEALTH = 1, FEED = 2, GET_POST = 3, CREATE = 4, LIKE = 5;

    private static final byte[] E_NOT_FOUND = err("not found"), E_INVALID_ID = err("invalid post id"),
            E_NO_POST = err("post not found"), E_MISSING_TOKEN = err("missing bearer token"),
            E_INVALID_TOKEN = err("invalid or expired token"), E_BAD_PAYLOAD = err("invalid token payload"),
            E_MALFORMED = err("malformed JSON body"), E_BODY_REQUIRED = err("body is required"),
            E_BODY_TOO_LONG = err("body must be at most 500 characters"), E_INTERNAL = err("internal server error"),
            E_BAD_REQUEST = err("bad request"), E_TOO_LARGE = err("request too large");
    private static final byte[] LIKED_NEW = Out.ascii("{\"liked\":true,\"already_liked\":false,\"post_id\":");
    private static final byte[] LIKED_AGAIN = Out.ascii("{\"liked\":true,\"already_liked\":true,\"post_id\":");
    private static final byte[] HEALTH_OK = Out.ascii("{\"status\":\"ok\",\"db\":\"ok\",\"uptime_s\":");

    private static byte[] err(String message) { return Out.ascii("{\"error\":\"" + message + "\"}"); }

    static final class Conn {
        final SocketChannel ch;
        SelectionKey key;
        byte[] left;
        ByteBuffer pending;
        boolean deferred;
        boolean closeAfterWrite, closed;
        Conn prev, next;

        Conn(SocketChannel ch) { this.ch = ch; }
    }

    private final Db db;
    private final Jwt jwt;
    private final Selector selector;
    private final ServerSocketChannel server;
    private final long startNanos = System.nanoTime();

    private final ByteBuffer in = ByteBuffer.allocateDirect(READ_CAP);
    private final byte[] rb = new byte[READ_CAP + MAX_HEAD + MAX_BODY];
    private final Out out = new Out(OUT_CAP);

    private final ArrayList<Conn> queue = new ArrayList<>(), batch = new ArrayList<>();
    private int[] sliceFrom = new int[64], sliceTo = new int[64];

    private Conn lruHead, lruTail;
    private int conns;
    private final int maxConns, evictTo;
    private long lastCheckpoint = System.nanoTime();

    Server(Db db, Jwt jwt, String host, int port, long maxFds) throws IOException {
        this.db = db;
        this.jwt = jwt;
        maxConns = (int) Math.max(64, Math.min(Integer.MAX_VALUE, maxFds) - 1024);
        evictTo = maxConns - Math.max(16, Math.min(1000, maxConns / 10));
        selector = Selector.open();
        server = ServerSocketChannel.open();
        server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        server.bind(new InetSocketAddress(host, port), 4096);
        server.configureBlocking(false);
        server.register(selector, SelectionKey.OP_ACCEPT, null);
    }

    void run() throws IOException {
        long batchNanos = Long.getLong("batch.micros", 2000) * 1000;
        int batchMin = Integer.getInteger("batch.min", 32);
        while (true) {
            int n = selector.select(this::onKey, 1000);
            commitPhase();
            long now = System.nanoTime();
            if (now - lastCheckpoint >= CHECKPOINT_NANOS) {
                lastCheckpoint = now;
                db.checkpointIfDirty();
            }
            if (n > 0 && n < batchMin && batchNanos > 0) LockSupport.parkNanos(batchNanos);
        }
    }

    private void onKey(SelectionKey k) {
        Object a = k.attachment();
        if (a == null) {
            accept();
            return;
        }
        Conn c = (Conn) a;
        try {
            int ready = k.readyOps();
            if ((ready & SelectionKey.OP_WRITE) != 0) onWritable(c);
            if (!c.closed && (ready & SelectionKey.OP_READ) != 0 && c.pending == null) onReadable(c);
        } catch (CancelledKeyException e) {
            close(c);
        } catch (Throwable t) {
            System.err.println("connection error: " + t);
            close(c);
        }
    }

    private void accept() {
        for (int n = 0; n < 512; n++) {
            SocketChannel ch;
            try {
                ch = server.accept();
            } catch (IOException e) {
                evict(Math.max(0, conns - 1000));
                return;
            }
            if (ch == null) break;
            Conn c = new Conn(ch);
            try {
                ch.configureBlocking(false);
                ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
                c.key = ch.register(selector, SelectionKey.OP_READ, c);
            } catch (IOException e) {
                try { ch.close(); } catch (IOException ignored) { }
                continue;
            }
            linkTail(c);
            conns++;
        }
        if (conns > maxConns) evict(evictTo);
    }

    private void evict(int target) {
        Conn c = lruHead;
        while (c != null && conns > target) {
            Conn next = c.next;
            if (c.pending == null && !c.deferred && c.left == null) close(c);
            c = next;
        }
    }

    private void close(Conn c) {
        if (c.closed) return;
        c.closed = true;
        unlink(c);
        conns--;
        c.left = null;
        c.pending = null;
        try { c.ch.close(); } catch (IOException ignored) { }
    }

    private void linkTail(Conn c) {
        c.prev = lruTail;
        c.next = null;
        if (lruTail != null) lruTail.next = c; else lruHead = c;
        lruTail = c;
    }

    private void unlink(Conn c) {
        if (c.prev != null) c.prev.next = c.next; else lruHead = c.next;
        if (c.next != null) c.next.prev = c.prev; else lruTail = c.prev;
        c.prev = c.next = null;
    }

    private void touch(Conn c) {
        if (lruTail == c) return;
        unlink(c);
        linkTail(c);
    }

    private void onReadable(Conn c) throws IOException {
        int have = c.left == null ? 0 : c.left.length;
        int room = Math.min(READ_CAP, rb.length - have);
        if (room <= 0) {
            close(c);
            return;
        }
        in.clear().limit(room);
        int n;
        try {
            n = c.ch.read(in);
        } catch (IOException e) {
            close(c);
            return;
        }
        if (n < 0) {
            close(c);
            return;
        }
        if (n == 0) return;
        touch(c);
        if (have > 0) System.arraycopy(c.left, 0, rb, 0, have);
        in.get(0, rb, have, n);
        c.left = null;
        if (c.deferred) {
            c.left = Arrays.copyOf(rb, have + n);
            return;
        }
        process(c, have + n, false);
    }

    private void onWritable(Conn c) throws IOException {
        ByteBuffer p = c.pending;
        try {
            c.ch.write(p);
        } catch (IOException e) {
            close(c);
            return;
        }
        if (p.hasRemaining()) return;
        c.pending = null;
        if (c.closeAfterWrite) {
            close(c);
            return;
        }
        c.key.interestOps(SelectionKey.OP_READ);
        resume(c);
    }

    private void resume(Conn c) {
        if (c.closed || c.left == null || c.deferred || c.pending != null) return;
        byte[] l = c.left;
        c.left = null;
        System.arraycopy(l, 0, rb, 0, l.length);
        process(c, l.length, false);
    }

    private void send(Conn c, int from, int to) {
        if (c.closed || from == to) return;
        ByteBuffer bb = out.buf;
        bb.limit(to).position(from);
        if (c.pending != null) {
            ByteBuffer p = ByteBuffer.allocate(c.pending.remaining() + bb.remaining());
            p.put(c.pending).put(bb).flip();
            c.pending = p;
        } else {
            try {
                c.ch.write(bb);
            } catch (IOException e) {
                bb.clear();
                close(c);
                return;
            }
            if (bb.hasRemaining()) {
                c.pending = ByteBuffer.allocate(bb.remaining()).put(bb).flip();
                c.key.interestOps(SelectionKey.OP_WRITE);
            } else if (c.closeAfterWrite) {
                bb.clear();
                close(c);
                return;
            }
        }
        bb.clear();
    }

    private void commitPhase() {
        while (!queue.isEmpty()) {
            batch.clear();
            batch.addAll(queue);
            queue.clear();
            int n = batch.size();
            if (sliceFrom.length < n) {
                sliceFrom = new int[n * 2];
                sliceTo = new int[n * 2];
            }
            out.pos = 0;
            for (int i = 0; i < n; i++) {
                Conn c = batch.get(i);
                c.deferred = false;
                sliceFrom[i] = out.pos;
                if (!c.closed && c.left != null) {
                    byte[] l = c.left;
                    c.left = null;
                    System.arraycopy(l, 0, rb, 0, l.length);
                    process(c, l.length, true);
                }
                sliceTo[i] = out.pos;
            }
            if (db.commit()) {
                for (int i = 0; i < n; i++) send(batch.get(i), sliceFrom[i], sliceTo[i]);
            } else {
                for (int i = 0; i < n; i++) close(batch.get(i));
            }
            out.pos = 0;
            for (int i = 0; i < n; i++) resume(batch.get(i));
        }
    }

    private int pathFrom, pathTo, authFrom, authTo, bodyFrom, bodyLen;

    private void process(Conn c, int end, boolean commitMode) {
        byte[] b = rb;
        int p = 0;
        while (p < end && !c.closed) {
            if (out.free() < OUT_RESERVE) {
                if (commitMode) break;
                flush(c);
                if (c.pending != null || c.closed) break;
            }
            int used = request(c, b, p, end, commitMode);
            if (used == 0) break;
            if (used < 0) {
                c.deferred = true;
                queue.add(c);
                break;
            }
            p += used;
            if (c.closeAfterWrite) {
                p = end;
                break;
            }
        }
        c.left = !c.closed && p < end ? Arrays.copyOfRange(b, p, end) : null;
        if (!commitMode) flush(c);
    }

    private void flush(Conn c) {
        int n = out.pos;
        out.pos = 0;
        send(c, 0, n);
    }

    private int request(Conn c, byte[] b, int p, int end, boolean commitMode) {
        int he = headerEnd(b, p, end);
        if (he < 0) {
            if (end - p > MAX_HEAD) return reject(c, Out.S431, E_TOO_LARGE, end - p);
            return 0;
        }
        int sp1 = indexOf(b, (byte) ' ', p, he);
        int sp2 = sp1 < 0 ? -1 : indexOf(b, (byte) ' ', sp1 + 1, he);
        int eol = sp2 < 0 ? -1 : indexOf(b, (byte) '\r', sp2 + 1, he);
        if (eol < 0 || eol - sp2 - 1 != 8 || !startsWith(b, sp2 + 1, "HTTP/1.")) {
            return reject(c, Out.S400, E_BAD_REQUEST, end - p);
        }
        boolean http11 = b[sp2 + 8] == '1';
        boolean isGet = sp1 - p == 3 && b[p] == 'G' && b[p + 1] == 'E' && b[p + 2] == 'T';
        boolean isPost = sp1 - p == 4 && b[p] == 'P' && b[p + 1] == 'O' && b[p + 2] == 'S' && b[p + 3] == 'T';
        pathFrom = sp1 + 1;
        pathTo = sp2;
        int q = indexOf(b, (byte) '?', pathFrom, pathTo);
        if (q >= 0) pathTo = q;

        authFrom = authTo = -1;
        long contentLength = 0;
        boolean connClose = false, connKeepAlive = false, chunked = false;
        int line = eol + 2;
        while (line < he - 2) {
            int le = indexOf(b, (byte) '\r', line, he);
            int colon = indexOf(b, (byte) ':', line, le);
            if (colon > line) {
                int vs = colon + 1, ve = le;
                while (vs < ve && (b[vs] == ' ' || b[vs] == '\t')) vs++;
                while (ve > vs && (b[ve - 1] == ' ' || b[ve - 1] == '\t')) ve--;
                int nl = colon - line;
                if (nl == 13 && nameIs(b, line, "authorization")) {
                    authFrom = vs;
                    authTo = ve;
                } else if (nl == 14 && nameIs(b, line, "content-length")) {
                    contentLength = 0;
                    if (vs == ve) return reject(c, Out.S400, E_BAD_REQUEST, end - p);
                    for (int k = vs; k < ve; k++) {
                        if (b[k] < '0' || b[k] > '9' || contentLength > MAX_BODY) {
                            return reject(c, b[k] < '0' || b[k] > '9' ? Out.S400 : Out.S413,
                                    b[k] < '0' || b[k] > '9' ? E_BAD_REQUEST : E_TOO_LARGE, end - p);
                        }
                        contentLength = contentLength * 10 + (b[k] - '0');
                    }
                } else if (nl == 10 && nameIs(b, line, "connection")) {
                    if (containsIgnoreCase(b, vs, ve, "close")) connClose = true;
                    if (containsIgnoreCase(b, vs, ve, "keep-alive")) connKeepAlive = true;
                } else if (nl == 17 && nameIs(b, line, "transfer-encoding")) {
                    chunked = true;
                }
            }
            line = le + 2;
        }
        if (chunked) return reject(c, Out.S400, E_BAD_REQUEST, end - p);
        if (contentLength > MAX_BODY) return reject(c, Out.S413, E_TOO_LARGE, end - p);
        int total = he - p + (int) contentLength;
        if (end - p < total) return 0;
        bodyFrom = he;
        bodyLen = (int) contentLength;

        int route = route(b, isGet, isPost);
        if (!commitMode && (route == CREATE || route == LIKE)) return -1;
        c.closeAfterWrite = http11 ? connClose : !connKeepAlive;
        out.close = c.closeAfterWrite;
        int mark = out.pos;
        try {
            handle(route, b);
        } catch (Throwable t) {
            System.err.println("request failed: " + t);
            out.pos = mark;
            out.respond(Out.S500, E_INTERNAL);
        }
        return total;
    }

    private int reject(Conn c, byte[] status, byte[] body, int consumed) {
        c.closeAfterWrite = true;
        out.close = true;
        out.respond(status, body);
        return Math.max(consumed, 1);
    }

    private int route(byte[] b, boolean isGet, boolean isPost) {
        int len = pathTo - pathFrom;
        if (isGet) {
            if (len == 7 && startsWith(b, pathFrom, "/health")) return HEALTH;
            if (len == 5 && startsWith(b, pathFrom, "/feed")) return FEED;
            if (len > 7 && startsWith(b, pathFrom, "/posts/") && indexOf(b, (byte) '/', pathFrom + 7, pathTo) < 0) {
                return GET_POST;
            }
            return NOT_FOUND;
        }
        if (isPost) {
            if (len == 6 && startsWith(b, pathFrom, "/posts")) return CREATE;
            if (len > 12 && startsWith(b, pathFrom, "/posts/") && endsWith(b, pathTo, "/like")
                    && indexOf(b, (byte) '/', pathFrom + 7, pathTo - 5) < 0) {
                return LIKE;
            }
        }
        return NOT_FOUND;
    }

    private void handle(int route, byte[] b) {
        switch (route) {
            case HEALTH -> health();
            case FEED -> db.feed(out);
            case GET_POST -> {
                long id = parseId(b, pathFrom + 7, pathTo);
                if (id == -1) out.respond(Out.S400, E_INVALID_ID);
                else if (id == -2 || !db.post(id, out)) out.respond(Out.S404, E_NO_POST);
            }
            case CREATE -> create(b);
            case LIKE -> like(b);
            default -> out.respond(Out.S404, E_NOT_FOUND);
        }
    }

    private void health() {
        String error = db.ping();
        if (error == null) {
            long up = (System.nanoTime() - startNanos) / 1_000_000_000L;
            out.head(Out.S200, HEALTH_OK.length + Out.digits(up) + 1);
            out.put(HEALTH_OK);
            out.putLong(up);
            out.put(CLOSE_BRACE);
        } else {
            byte[] body = ("{\"status\":\"degraded\",\"db\":\"unreachable\",\"error\":" + quote(error) + "}")
                    .getBytes(StandardCharsets.UTF_8);
            out.respond(Out.S503, body);
        }
    }

    private boolean auth(byte[] b) {
        int r = jwt.verify(b, authFrom, authTo);
        if (r == Jwt.OK) return true;
        out.respond(Out.S401, r == Jwt.MISSING ? E_MISSING_TOKEN : r == Jwt.INVALID ? E_INVALID_TOKEN : E_BAD_PAYLOAD);
        return false;
    }

    private void create(byte[] b) {
        if (!auth(b)) return;
        Object doc;
        try {
            doc = Json.parse(Json.utf8(b, bodyFrom, bodyLen));
        } catch (Json.Malformed e) {
            out.respond(Out.S400, E_MALFORMED);
            return;
        }
        if (!(Json.get(doc, "body") instanceof String raw)) {
            out.respond(Out.S400, E_BODY_REQUIRED);
            return;
        }
        String body = trim(raw);
        if (body.isEmpty()) {
            out.respond(Out.S400, E_BODY_REQUIRED);
            return;
        }
        if (body.codePointCount(0, body.length()) > 500) {
            out.respond(Out.S400, E_BODY_TOO_LONG);
            return;
        }
        db.insertPost(jwt.userId, body.getBytes(StandardCharsets.UTF_8), jwt.username, out);
    }

    private void like(byte[] b) {
        if (!auth(b)) return;
        long id = parseId(b, pathFrom + 7, pathTo - 5);
        if (id == -1) {
            out.respond(Out.S400, E_INVALID_ID);
            return;
        }
        int status = id == -2 ? 404 : db.like(jwt.userId, id);
        if (status == 404) {
            out.respond(Out.S404, E_NO_POST);
            return;
        }
        byte[] prefix = status == 201 ? LIKED_NEW : LIKED_AGAIN;
        out.head(status == 201 ? Out.S201 : Out.S200, prefix.length + Out.digits(id) + 1);
        out.put(prefix);
        out.putLong(id);
        out.put(CLOSE_BRACE);
    }

    private static final byte[] CLOSE_BRACE = {'}'};

    private static long parseId(byte[] b, int from, int to) {
        if (from >= to) return -1;
        long v = 0;
        boolean overflow = false;
        for (int k = from; k < to; k++) {
            int d = b[k] - '0';
            if (d < 0 || d > 9) return -1;
            if (!overflow) {
                if (v > (Long.MAX_VALUE - d) / 10) overflow = true;
                else v = v * 10 + d;
            }
        }
        if (overflow) return -2;
        return v > 0 ? v : -1;
    }

    private static boolean isJsSpace(char c) {
        return c == ' ' || (c >= '\t' && c <= '\r') || c == ' ' || c == ' '
                || (c >= ' ' && c <= ' ') || c == ' ' || c == ' ' || c == ' '
                || c == ' ' || c == '　' || c == '﻿';
    }

    private static String trim(String s) {
        int a = 0, z = s.length();
        while (a < z && isJsSpace(s.charAt(a))) a++;
        while (z > a && isJsSpace(s.charAt(z - 1))) z--;
        return s.substring(a, z);
    }

    private static String quote(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2).append('"');
        for (int k = 0; k < s.length(); k++) {
            char c = s.charAt(k);
            if (c == '"' || c == '\\') sb.append('\\').append(c);
            else if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
            else sb.append(c);
        }
        return sb.append('"').toString();
    }

    private static int headerEnd(byte[] b, int p, int end) {
        for (int k = p + 3; k < end; k++) {
            if (b[k] == '\n' && b[k - 1] == '\r' && b[k - 2] == '\n' && b[k - 3] == '\r') return k + 1;
        }
        return -1;
    }

    private static int indexOf(byte[] b, byte x, int from, int to) {
        for (int k = from; k < to; k++) if (b[k] == x) return k;
        return -1;
    }

    private static boolean startsWith(byte[] b, int at, String s) {
        for (int k = 0; k < s.length(); k++) if (b[at + k] != s.charAt(k)) return false;
        return true;
    }

    private static boolean endsWith(byte[] b, int to, String s) {
        return startsWith(b, to - s.length(), s);
    }

    private static boolean nameIs(byte[] b, int at, String lower) {
        for (int k = 0; k < lower.length(); k++) if ((b[at + k] | 0x20) != lower.charAt(k)) return false;
        return true;
    }

    private static boolean containsIgnoreCase(byte[] b, int from, int to, String lower) {
        int n = lower.length();
        for (int k = from; k + n <= to; k++) if (nameIs(b, k, lower)) return true;
        return false;
    }
}
