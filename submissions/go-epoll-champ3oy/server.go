package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"
)

// One epoll loop on one OS thread serves every connection. Idle connections cost a conn struct
// and the kernel socket: input is read into one shared buffer and only a partial request is
// copied into the connection, and output only stays on the connection when the socket is full.
//
// Writes (likes and new posts) are group-committed: the loop collects them while it works
// through one batch of epoll events, runs them in a single transaction, and only then sends
// their responses, so every 201 follows its COMMIT. WAL checkpoints run on a second thread.

type conn struct {
	in, out      []byte
	fd, gen      int32 // gen tells a reused fd number apart from the connection that had it before
	open         bool
	blocked      bool // a write is waiting for the group commit; later requests wait behind it
	closeAfter   bool
	pollOut      bool
	sentContinue bool
}

type writeOp struct {
	fd, gen        int32
	create         bool
	userID, postID int64
	body, author   string
	connMode       int
	status         int
	lo, hi         int // the rendered post in server.stage
}

type server struct {
	epfd, lfd, spare int
	conns            []conn
	events           []syscall.EpollEvent
	nextGen          int32
	rbuf, wbuf       []byte
	stage            []byte
	hdr              []byte
	ops, opsSpare    []writeOp
	jwt              *verifier
	start            time.Time
	checkpointReq    chan struct{}
	checkpointPipe   int // read end; readable when a checkpoint has finished
	checkpointing    bool
}

const (
	feedRoom = headerRoom + 256<<10 // 20 posts, every byte escaped in the worst case
	postRoom = headerRoom + 16<<10
)

func newServer(host, port, secret string) (*server, error) {
	s := &server{
		conns:  make([]conn, 4096),
		events: make([]syscall.EpollEvent, 1024),
		rbuf:   make([]byte, 64<<10),
		wbuf:   make([]byte, 0, 1<<20),
		stage:  make([]byte, 0, 64<<10),
		hdr:    make([]byte, 0, headerRoom),
		jwt:    newVerifier(secret),
		start:  time.Now(),
	}
	if err := s.listen(host, port); err != nil {
		return nil, err
	}
	var err error
	if s.epfd, err = syscall.EpollCreate1(syscall.EPOLL_CLOEXEC); err != nil {
		return nil, err
	}
	ev := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(s.lfd)}
	if err = syscall.EpollCtl(s.epfd, syscall.EPOLL_CTL_ADD, s.lfd, &ev); err != nil {
		return nil, err
	}
	var p [2]int
	if err = syscall.Pipe2(p[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		return nil, err
	}
	s.checkpointPipe, s.checkpointReq = p[0], make(chan struct{}, 1)
	ev = syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(p[0])}
	if err = syscall.EpollCtl(s.epfd, syscall.EPOLL_CTL_ADD, p[0], &ev); err != nil {
		return nil, err
	}
	go s.checkpointer(p[1])
	// Held in reserve so that at the descriptor limit we can still accept and close.
	s.spare, _ = syscall.Open("/dev/null", syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
	return s, nil
}

func (s *server) listen(host, portStr string) error {
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("bad PORT %q", portStr)
	}
	ips, err := net.LookupIP(host)
	if err != nil || len(ips) == 0 {
		return fmt.Errorf("bad HOST %q: %v", host, err)
	}
	family := syscall.AF_INET
	var sa syscall.Sockaddr
	if ip4 := ips[0].To4(); ip4 != nil {
		a := &syscall.SockaddrInet4{Port: port}
		copy(a.Addr[:], ip4)
		sa = a
	} else {
		a := &syscall.SockaddrInet6{Port: port}
		copy(a.Addr[:], ips[0].To16())
		sa, family = a, syscall.AF_INET6
	}
	fd, err := syscall.Socket(family, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	if err = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		return err
	}
	if err = syscall.Bind(fd, sa); err != nil {
		return err
	}
	if err = syscall.Listen(fd, 65535); err != nil { // the kernel caps it at net.core.somaxconn
		return err
	}
	s.lfd = fd
	return nil
}

func (s *server) run() {
	for {
		n, err := syscall.EpollWait(s.epfd, s.events, -1)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			log.Fatalf("epoll_wait: %v", err)
		}
		for i := 0; i < n; i++ {
			ev := s.events[i]
			fd := int(ev.Fd)
			if fd == s.lfd {
				s.accept()
				continue
			}
			if fd == s.checkpointPipe {
				s.checkpointDone()
				continue
			}
			c := &s.conns[fd]
			if !c.open || c.gen != ev.Pad {
				continue // closed earlier in this batch
			}
			if ev.Events&syscall.EPOLLOUT != 0 {
				s.flush(c)
			}
			if c.open && ev.Events&(syscall.EPOLLIN|syscall.EPOLLHUP|syscall.EPOLLERR) != 0 {
				s.readable(c)
			}
		}
		for len(s.ops) > 0 && !s.checkpointing {
			s.commit()
		}
	}
}

func (s *server) accept() {
	for {
		fd, _, err := syscall.Accept4(s.lfd, syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC)
		if err != nil {
			switch err {
			case syscall.EINTR, syscall.ECONNABORTED:
				continue
			case syscall.EMFILE, syscall.ENFILE:
				// Out of descriptors: shed one waiting client rather than spin on the listener.
				if s.spare >= 0 {
					syscall.Close(s.spare)
					if fd, _, err := syscall.Accept4(s.lfd, syscall.SOCK_CLOEXEC); err == nil {
						syscall.Close(fd)
					}
					s.spare, _ = syscall.Open("/dev/null", syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
				}
			}
			return
		}
		syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
		if fd >= len(s.conns) {
			grown := make([]conn, max(fd+1, 2*len(s.conns)))
			copy(grown, s.conns)
			s.conns = grown
		}
		s.nextGen++
		c := &s.conns[fd]
		*c = conn{fd: int32(fd), gen: s.nextGen, open: true}
		ev := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(fd), Pad: c.gen}
		if syscall.EpollCtl(s.epfd, syscall.EPOLL_CTL_ADD, fd, &ev) != nil {
			s.close(c)
		}
	}
}

func (s *server) close(c *conn) {
	syscall.Close(int(c.fd)) // also removes it from the epoll set
	*c = conn{fd: c.fd, gen: c.gen}
}

func (s *server) setPollOut(c *conn, on bool) {
	ev := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: c.fd, Pad: c.gen}
	if on {
		ev.Events |= syscall.EPOLLOUT
	}
	c.pollOut = on
	syscall.EpollCtl(s.epfd, syscall.EPOLL_CTL_MOD, int(c.fd), &ev)
}

func (s *server) readable(c *conn) {
	n, err := syscall.Read(int(c.fd), s.rbuf)
	if err == syscall.EAGAIN || err == syscall.EINTR {
		return
	}
	if n <= 0 || err != nil {
		s.close(c)
		return
	}
	if len(c.in) > 0 {
		c.in = append(c.in, s.rbuf[:n]...)
		s.process(c, c.in, true)
	} else {
		s.process(c, s.rbuf[:n], false)
	}
}

// process handles every complete request in data (c.in when owned, else the shared read
// buffer), stopping early at a write that has to wait for the group commit.
func (s *server) process(c *conn, data []byte, owned bool) {
	out := s.wbuf[:0]
	off := 0
loop:
	for c.open && !c.blocked && !c.closeAfter {
		var r request
		used, res := parseRequest(data[off:], &r)
		switch res {
		case parseOK:
			off += used
			c.sentContinue = false
			out = s.handle(c, &r, out)
		case parseIncomplete:
			break loop
		case parseNeedContinue:
			if !c.sentContinue {
				out = append(out, continueResponse...)
				c.sentContinue = true
			}
			break loop
		default:
			status, body := 400, errBadRequest
			if res == parseTooLarge {
				status, body = 413, errTooLarge
			} else if res == parseUnsupported {
				status, body = 501, errUnsupported
			}
			out = appendResponse(out, status, body, connClose)
			c.closeAfter = true
			off = len(data)
		}
	}

	rest := data[off:]
	switch {
	case len(rest) == 0:
		c.in = c.in[:0]
		if cap(c.in) > 64<<10 {
			c.in = nil
		}
	case owned:
		c.in = c.in[:copy(c.in, rest)]
	default:
		c.in = append(c.in[:0], rest...)
	}

	s.send(c, out)
	if c.open && c.closeAfter && !c.blocked && len(c.out) == 0 {
		s.close(c)
	}
}

// send writes out to the socket, keeping whatever doesn't fit until the socket is writable.
func (s *server) send(c *conn, out []byte) {
	if len(out) == 0 || !c.open {
		return
	}
	if len(c.out) > 0 {
		c.out = append(c.out, out...)
		return
	}
	for len(out) > 0 {
		n, err := syscall.Write(int(c.fd), out)
		if err == syscall.EINTR {
			continue
		}
		if err == syscall.EAGAIN {
			break
		}
		if err != nil {
			s.close(c)
			return
		}
		out = out[n:]
	}
	if len(out) > 0 {
		c.out = append([]byte(nil), out...)
		s.setPollOut(c, true)
	}
}

func (s *server) flush(c *conn) {
	for len(c.out) > 0 {
		n, err := syscall.Write(int(c.fd), c.out)
		if err == syscall.EINTR {
			continue
		}
		if err == syscall.EAGAIN {
			return
		}
		if err != nil {
			s.close(c)
			return
		}
		c.out = c.out[n:]
	}
	c.out = nil
	if c.pollOut {
		s.setPollOut(c, false)
	}
	if c.closeAfter && !c.blocked {
		s.close(c)
	}
}

// reserve makes sure out has room for need more bytes, flushing what it holds if not.
func (s *server) reserve(c *conn, out []byte, need int) []byte {
	if cap(out)-len(out) >= need {
		return out
	}
	s.send(c, out)
	if cap(s.wbuf) < need {
		s.wbuf = make([]byte, 0, need)
	}
	return s.wbuf[:0]
}

// --- routing --------------------------------------------------------------------------------

const (
	renderFeed = iota
	renderPost
)

func (s *server) handle(c *conn, r *request, out []byte) []byte {
	mode := connKeep
	if !r.keepAlive {
		mode = connClose
		c.closeAfter = true
	} else if r.http10 {
		mode = conn10
	}
	path := string(r.path) // the compiler compares these without allocating
	get, post := string(r.method) == "GET", string(r.method) == "POST"

	switch {
	case get && path == "/feed":
		return s.render(c, out, mode, renderFeed, 0)
	case get && path == "/health":
		if msg, ok := dbPing(); !ok {
			b, _ := json.Marshal(struct {
				Status string `json:"status"`
				DB     string `json:"db"`
				Error  string `json:"error"`
			}{"degraded", "unreachable", msg})
			return appendResponse(out, 503, b, mode)
		}
		body := append(s.hdr[:0], `{"status":"ok","db":"ok","uptime_s":`...)
		body = strconv.AppendInt(body, int64(time.Since(s.start)/time.Second), 10)
		body = append(body, '}')
		return appendResponse(out, 200, body, mode)
	case get && strings.HasPrefix(path, "/posts/") && strings.IndexByte(path[7:], '/') < 0:
		id, valid, tooBig := parseID(r.path[7:])
		switch {
		case !valid:
			return appendResponse(out, 400, errInvalidID, mode)
		case tooBig:
			return appendResponse(out, 404, errPostNotFound, mode)
		}
		return s.render(c, out, mode, renderPost, id)
	case post && path == "/posts":
		return s.createPost(c, r, out, mode)
	case post && len(path) >= 12 && strings.HasPrefix(path, "/posts/") && strings.HasSuffix(path, "/like") &&
		strings.IndexByte(path[7:len(path)-5], '/') < 0:
		return s.like(c, r, r.path[7:len(path)-5], out, mode)
	}
	return appendResponse(out, 404, errNotFound, mode)
}

// render runs a read query in C, which writes the JSON body straight into out behind room
// left for the headers; the body then moves up against the headers.
func (s *server) render(c *conn, out []byte, mode, kind int, id int64) []byte {
	need := feedRoom
	if kind == renderPost {
		need = postRoom
	}
	out = s.reserve(c, out, need)
	start := len(out)
	full := out[:cap(out)]
	var n int
	if kind == renderFeed {
		n = dbFeed(full[start+headerRoom:])
	} else {
		n = dbPost(id, full[start+headerRoom:])
	}
	switch {
	case n < 0:
		return appendResponse(out, 500, errInternal, mode)
	case n == 0:
		return appendResponse(out, 404, errPostNotFound, mode)
	}
	hdr := appendHeader(s.hdr[:0], 200, n, mode)
	copy(full[start+len(hdr):], full[start+headerRoom:start+headerRoom+n])
	copy(full[start:], hdr)
	return full[:start+len(hdr)+n]
}

func (s *server) createPost(c *conn, r *request, out []byte, mode int) []byte {
	userID, username, authErr := s.jwt.auth(r.auth, r.hasAuth)
	if authErr != nil {
		return appendResponse(out, 401, authErr, mode)
	}
	var v any
	if json.Unmarshal(r.body, &v) != nil {
		return appendResponse(out, 400, errMalformedJSON, mode)
	}
	obj, _ := v.(map[string]any)
	body, ok := obj["body"].(string)
	if !ok {
		return appendResponse(out, 400, errBodyRequired, mode)
	}
	body = strings.TrimFunc(body, isJSSpace)
	if body == "" {
		return appendResponse(out, 400, errBodyRequired, mode)
	}
	if utf8.RuneCountInString(body) > 500 {
		return appendResponse(out, 400, errBodyTooLong, mode)
	}
	s.ops = append(s.ops, writeOp{fd: c.fd, gen: c.gen, create: true, userID: userID,
		body: body, author: username, connMode: mode})
	c.blocked = true
	return out
}

func (s *server) like(c *conn, r *request, rawID []byte, out []byte, mode int) []byte {
	userID, _, authErr := s.jwt.auth(r.auth, r.hasAuth) // auth before the id, per the spec
	if authErr != nil {
		return appendResponse(out, 401, authErr, mode)
	}
	postID, valid, tooBig := parseID(rawID)
	switch {
	case !valid:
		return appendResponse(out, 400, errInvalidID, mode)
	case tooBig:
		return appendResponse(out, 404, errPostNotFound, mode)
	}
	s.ops = append(s.ops, writeOp{fd: c.fd, gen: c.gen, userID: userID, postID: postID, connMode: mode})
	c.blocked = true
	return out
}

// isJSSpace is JavaScript's String.prototype.trim() set (WhiteSpace + LineTerminator).
func isJSSpace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0xA0, 0x1680, 0x2028, 0x2029, 0x202F, 0x205F, 0x3000, 0xFEFF:
		return true
	}
	return r >= 0x2000 && r <= 0x200A
}

// --- group commit ---------------------------------------------------------------------------

// commit runs every queued write in one transaction, then answers them. Requests that were
// pipelined behind a write run afterwards and may queue the next batch.
func (s *server) commit() {
	ops := s.ops
	s.ops = s.opsSpare[:0]
	stage := s.stage[:0]

	ok := dbBegin()
	for i := range ops {
		op := &ops[i]
		if !ok {
			break
		}
		if op.create {
			room := 6*(len(op.body)+len(op.author)) + 256
			if cap(stage)-len(stage) < room {
				grown := make([]byte, len(stage), 2*cap(stage)+room)
				copy(grown, stage)
				stage = grown
			}
			start := len(stage)
			n := dbCreate(op.userID, op.body, op.author, stage[start:cap(stage)])
			if n < 0 {
				op.status = 500
				continue
			}
			op.status, op.lo, op.hi = 201, start, start+n
			stage = stage[:start+n]
			continue
		}
		switch dbLike(op.userID, op.postID) {
		case 1:
			op.status = 201
		case 2:
			op.status = 200
		case 0:
			op.status = 404
		default:
			op.status = 500
		}
	}
	if ok && !dbCommit() {
		dbRollback()
		ok = false
	}
	s.stage = stage

	for i := range ops {
		op := &ops[i]
		if !ok {
			op.status = 500
		}
		c := &s.conns[op.fd]
		if !c.open || c.gen != op.gen {
			continue // the client left; its write is committed anyway
		}
		c.blocked = false
		out := s.wbuf[:0]
		switch {
		case op.status == 404:
			out = appendResponse(out, 404, errPostNotFound, op.connMode)
		case op.status >= 500:
			out = appendResponse(out, 500, errInternal, op.connMode)
		case op.create:
			out = appendResponse(out, 201, stage[op.lo:op.hi], op.connMode)
		default:
			body := append(s.hdr[:0], `{"liked":true,"already_liked":`...)
			body = strconv.AppendBool(body, op.status == 200)
			body = append(body, `,"post_id":`...)
			body = strconv.AppendInt(body, op.postID, 10)
			body = append(body, '}')
			out = appendResponse(out, op.status, body, op.connMode)
		}
		s.send(c, out)
		switch {
		case !c.open:
		case c.closeAfter:
			if len(c.out) == 0 {
				s.close(c)
			}
		case len(c.in) > 0 && bytes.Contains(c.in, crlfcrlf):
			s.process(c, c.in, true)
		}
	}
	clear(ops) // drop the strings
	s.opsSpare = ops[:0]
	if dbWALFrames() >= checkpointFrames {
		s.checkpointing = true
		s.checkpointReq <- struct{}{}
	}
}

// SQLite's default autocheckpoint threshold: about 4 MB of WAL.
const checkpointFrames = 1000

// checkpointer runs WAL checkpoints on its own thread and connection. While one runs the
// event loop doesn't commit, so the checkpoint catches up with the whole WAL and the next
// write rewinds it to the start. Each finished checkpoint writes a byte to the pipe.
func (s *server) checkpointer(done int) {
	runtime.LockOSThread()
	one := []byte{1}
	for range s.checkpointReq {
		dbCheckpoint()
		syscall.Write(done, one)
	}
}

func (s *server) checkpointDone() {
	var buf [64]byte
	syscall.Read(s.checkpointPipe, buf[:])
	s.checkpointing = false
}
