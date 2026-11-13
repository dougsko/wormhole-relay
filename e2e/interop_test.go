//go:build e2e

// Package e2e contains end-to-end interoperability tests for the Go
// transit relay. Every transfer in this suite is forced through the Go
// relay: both the Python client (--no-listen) and the wormhole-william Go
// library (NoListen) are configured so that the relay is the *only*
// transit candidate. The transfers are additionally proven to have gone
// through the relay by checking the relay's usage database for a 'happy'
// pairing with the expected byte count.
//
// Topology per test (all processes on 127.0.0.1):
//
//	wormhole-mailbox (Go)  <-- rendezvous -->  clients
//	wormhole-relay   (Go)  <-- transit    -->  clients
//
// Requirements (tests skip gracefully if missing, unless
// WORMHOLE_E2E_REQUIRE=1 is set, in which case they fail):
//
//   - a Python environment with magic-wormhole installed
//     (or point WORMHOLE_E2E_PYTHON at the 'wormhole' entry script)
//   - a checkout of wormhole-mailbox as a sibling of this repository
//     (or set WORMHOLE_E2E_MAILBOX to its path)
//   - a checkout of wormhole-william as a sibling of this repository
//     (see the replace directive in e2e/go.mod)
//
// Run from the e2e directory with:
//
//	go test -tags=e2e -v .
//
// Note: text messages ride the mailbox, not the transit relay, so there is
// no text case here; the mailbox repo's e2e suite covers those.

package e2e

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite" // pure-Go sqlite driver for the usage-db assertion

	"github.com/dougsko/wormhole-william/wormhole"
)

const (
	testTimeout     = 90 * time.Second
	serverReadyWait = 15 * time.Second
)

// ---------- harness ----------

type stack struct {
	relayURL    string // ws://127.0.0.1:PORT/v1 (mailbox)
	transitAddr string // 127.0.0.1:PORT (relay)
	mboxLog     string
	relayLog    string
	usageDB     string

	mboxCmd  *exec.Cmd
	relayCmd *exec.Cmd
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitForTCP(t *testing.T, addr string, d time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(d)
	var err error
	for time.Now().Before(deadline) {
		conn, derr := net.DialTimeout("tcp", addr, 250*time.Millisecond)
		if derr == nil {
			_ = conn.Close()
			return nil
		}
		err = derr
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s: %v", addr, err)
}

// waitForWebSocket confirms the mailbox answers on /v1 (the websocket
// handshake needs an HTTP server that is actually serving, not just a
// socket in listen state).
func waitForWebSocket(t *testing.T, relayURL string, d time.Duration) error {
	t.Helper()
	// relayURL is ws://host/v1; probe with a plain HTTP GET and accept any
	// response (the upgrade will be rejected, but that proves the handler
	// is mounted).
	httpURL := "http://" + strings.TrimPrefix(relayURL, "ws://")
	deadline := time.Now().Add(d)
	var err error
	for time.Now().Before(deadline) {
		resp, derr := http.Get(httpURL) //nolint:gosec // localhost only
		if derr == nil {
			_ = resp.Body.Close()
			return nil
		}
		err = derr
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for mailbox at %s: %v", relayURL, err)
}

var mailboxBinOnce = struct {
	path string
	err  error
	done bool
}{}

var relayBinOnce = struct {
	path string
	err  error
	done bool
}{}

func buildMailbox(t *testing.T) string {
	t.Helper()
	if mailboxBinOnce.done {
		if mailboxBinOnce.err != nil {
			t.Fatalf("mailbox-server build failed earlier: %v", mailboxBinOnce.err)
		}
		return mailboxBinOnce.path
	}
	mailboxRepo := os.Getenv("WORMHOLE_E2E_MAILBOX")
	if mailboxRepo == "" {
		var err error
		mailboxRepo, err = filepath.Abs("../../wormhole-mailbox")
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(mailboxRepo, "cmd", "mailbox-server")); err != nil {
		if os.Getenv("WORMHOLE_E2E_REQUIRE") == "1" {
			t.Fatalf("wormhole-mailbox checkout not found at %s (set WORMHOLE_E2E_MAILBOX)", mailboxRepo)
		}
		t.Skipf("wormhole-mailbox checkout not found at %s; set WORMHOLE_E2E_MAILBOX (or WORMHOLE_E2E_REQUIRE=1)", mailboxRepo)
	}
	tmp, err := os.MkdirTemp("", "mailbox-server-bin")
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(tmp, "mailbox-server")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/mailbox-server")
	cmd.Dir = mailboxRepo
	out, err := cmd.CombinedOutput()
	if err != nil {
		mailboxBinOnce.err = fmt.Errorf("go build ./cmd/mailbox-server in %s: %v\n%s", mailboxRepo, err, out)
		t.Fatalf("%v", mailboxBinOnce.err)
	}
	mailboxBinOnce.path = bin
	mailboxBinOnce.done = true
	return bin
}

func buildRelay(t *testing.T) string {
	t.Helper()
	if relayBinOnce.done {
		if relayBinOnce.err != nil {
			t.Fatalf("relay-server build failed earlier: %v", relayBinOnce.err)
		}
		return relayBinOnce.path
	}
	tmp, err := os.MkdirTemp("", "relay-server-bin")
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(tmp, "relay-server")
	repoRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/relay-server")
	cmd.Dir = repoRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		relayBinOnce.err = fmt.Errorf("go build ./cmd/relay-server: %v\n%s", err, out)
		t.Fatalf("%v", relayBinOnce.err)
	}
	relayBinOnce.path = bin
	relayBinOnce.done = true
	return bin
}

// pythonTool locates the 'wormhole' entry script; returns "" if absent.
func pythonTool() string {
	if p := os.Getenv("WORMHOLE_E2E_PYTHON"); p != "" {
		return p
	}
	for _, cand := range []string{
		"/tmp/wormhole-venv/bin/wormhole",
		"wormhole", // PATH lookup
	} {
		if p, err := exec.LookPath(cand); err == nil {
			return p
		}
	}
	return ""
}

func requirePython(t *testing.T) string {
	t.Helper()
	wormhole := pythonTool()
	if wormhole == "" {
		if os.Getenv("WORMHOLE_E2E_REQUIRE") == "1" {
			t.Fatal("python magic-wormhole client not found; set WORMHOLE_E2E_PYTHON")
		}
		t.Skip("python magic-wormhole client not found; set WORMHOLE_E2E_PYTHON (or WORMHOLE_E2E_REQUIRE=1)")
	}
	// sanity check: the venv must actually have magic-wormhole importable
	out, err := exec.Command(wormhole, "--version").CombinedOutput()
	if err != nil {
		if os.Getenv("WORMHOLE_E2E_REQUIRE") == "1" {
			t.Fatalf("python wormhole client %q not runnable: %v\n%s", wormhole, err, out)
		}
		t.Skipf("python wormhole client %q not runnable: %v", wormhole, err)
	}
	return wormhole
}

// startStack boots the Go mailbox-server and the Go relay-server on
// 127.0.0.1 with random free ports, using fresh temporary databases.
func startStack(t *testing.T) *stack {
	t.Helper()
	python := requirePython(t)
	_ = python

	dir := t.TempDir()
	mboxLog := filepath.Join(dir, "mailbox.log")
	relayLog := filepath.Join(dir, "relay.log")
	usageDB := filepath.Join(dir, "relay-usage.sqlite")

	mboxPort := freePort(t)
	relayPort := freePort(t)

	mboxCmd := exec.Command(buildMailbox(t),
		"--port", fmt.Sprintf("tcp:%d:interface=127.0.0.1", mboxPort),
		"--channel-db", filepath.Join(dir, "relay.sqlite"),
	)
	mboxCmd.Dir = dir // keep cwd clean (no pid files etc.)
	mboxCmd.Stdout = mustAppendFile(t, mboxLog)
	mboxCmd.Stderr = mboxCmd.Stdout
	if err := mboxCmd.Start(); err != nil {
		t.Fatal(err)
	}

	relayCmd := exec.Command(buildRelay(t),
		"--port", fmt.Sprintf("tcp:%d:interface=127.0.0.1", relayPort),
		"--usage-db", usageDB,
	)
	relayCmd.Dir = dir // keep cwd clean (no pid files etc.)
	relayCmd.Stdout = mustAppendFile(t, relayLog)
	relayCmd.Stderr = relayCmd.Stdout
	if err := relayCmd.Start(); err != nil {
		_ = mboxCmd.Process.Kill()
		t.Fatalf("start relay-server: %v", err)
	}

	s := &stack{
		relayURL:    fmt.Sprintf("ws://127.0.0.1:%d/v1", mboxPort),
		transitAddr: fmt.Sprintf("127.0.0.1:%d", relayPort),
		mboxLog:     mboxLog,
		relayLog:    relayLog,
		usageDB:     usageDB,
		mboxCmd:     mboxCmd,
		relayCmd:    relayCmd,
	}
	t.Cleanup(func() {
		if s.mboxCmd.Process != nil {
			_ = s.mboxCmd.Process.Kill()
			_ = s.mboxCmd.Wait()
		}
		if s.relayCmd.Process != nil {
			_ = s.relayCmd.Process.Kill()
			_ = s.relayCmd.Wait()
		}
	})

	// surface server logs when the test fails
	dumpLogOnFailure := func(name, path string) {
		t.Cleanup(func() {
			if !t.Failed() {
				return
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Logf("could not read %s log %s: %v", name, path, err)
				return
			}
			const tail = 4000
			if len(data) > tail {
				data = data[len(data)-tail:]
			}
			t.Logf("=== %s log (tail) ===\n%s", name, data)
		})
	}
	dumpLogOnFailure("mailbox", mboxLog)
	dumpLogOnFailure("relay", relayLog)

	if err := waitForWebSocket(t, s.relayURL, serverReadyWait); err != nil {
		t.Fatalf("mailbox did not come up: %v (log: %s)", err, mboxLog)
	}
	if err := waitForTCP(t, s.transitAddr, serverReadyWait); err != nil {
		t.Fatalf("relay did not come up: %v (log: %s)", err, relayLog)
	}
	return s
}

func mustAppendFile(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func pythonCmd(ctx context.Context, python string, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, python, args...)
}

// pyArgs points the Python client at the local mailbox and relay.
// --no-listen makes the relay the ONLY transit candidate for either side.
func (s *stack) pyArgs(extra ...string) []string {
	args := []string{"--relay-url", s.relayURL,
		"--transit-helper", "tcp:" + s.transitAddr}
	return append(args, extra...)
}

func goClient(s *stack) *wormhole.Client {
	return &wormhole.Client{
		RendezvousURL:       s.relayURL,
		TransitRelayAddress: s.transitAddr,
		// no local listening socket: the relay is the only transit path
		NoListen:                 true,
		PassPhraseComponentLength: 2,
	}
}

// ---------- payload fixtures ----------

func sha256File(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// makeDirectory builds a nested tree inside root/<dirname> and returns
// (root, dirname, map[relpath]sha256hex).
func makeDirectory(t *testing.T, dirname string) (string, string, map[string]string) {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, dirname)
	m := map[string]string{}
	put := func(rel string, data []byte) {
		path := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		h := sha256.Sum256(data)
		m[filepath.ToSlash(filepath.Join(dirname, rel))] = hex.EncodeToString(h[:])
	}
	put("a.json", []byte(`{"hello":"world","n":12345}`+"\n"))
	put("sub/b.txt", []byte("hello world\n"))
	blob := make([]byte, 256*1024)
	if _, err := rand.Read(blob); err != nil {
		t.Fatal(err)
	}
	put("blob.bin", blob)
	put("sub/deep/c.dat", []byte{0x00, 0x01, 0x02, 0x7f, 0x80, 0xff})
	return root, dirname, m
}

// walkChecksums walks dir and returns map[relpath]sha256hex, relativized
// against the parent of dir (so paths start with "<dirname>/").
func walkChecksums(t *testing.T, dir string) map[string]string {
	t.Helper()
	base := filepath.Dir(dir)
	m := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(base, path)
		if err != nil {
			return err
		}
		sum, err := func() (string, error) {
			f, err := os.Open(path)
			if err != nil {
				return "", err
			}
			defer f.Close()
			h := sha256.New()
			_, err = io.Copy(h, f)
			return hex.EncodeToString(h.Sum(nil)), err
		}()
		if err != nil {
			t.Fatal(err)
		}
		m[filepath.ToSlash(rel)] = sum
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func compareTrees(t *testing.T, want, got map[string]string) {
	t.Helper()
	for _, k := range sortedKeys(want) {
		g, ok := got[k]
		if !ok {
			t.Errorf("missing file in received tree: %s", k)
			continue
		}
		if g != want[k] {
			t.Errorf("checksum mismatch for %s", k)
		}
	}
	for _, k := range sortedKeys(got) {
		if _, ok := want[k]; !ok {
			t.Errorf("unexpected file in received tree: %s", k)
		}
	}
}

// ---------- relay usage DB assertion ----------

type usageRow struct {
	result    string
	totalBytes int64
}

// usageRows polls the relay's usage database until at least one usage
// record shows up (the record is written when the connection closes) or
// the deadline passes. Returns all rows found.
func usageRows(t *testing.T, path string, d time.Duration) []usageRow {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		db, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=busy_timeout(1000)")
		if err == nil {
			rows, qerr := db.Query("SELECT result, total_bytes FROM usage")
			var out []usageRow
			if qerr == nil {
				for rows.Next() {
					var r usageRow
					if err := rows.Scan(&r.result, &r.totalBytes); err == nil {
						out = append(out, r)
					}
				}
				_ = rows.Close()
			}
			_ = db.Close()
			if len(out) > 0 {
				return out
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

// requireHappyPairing asserts the relay recorded at least one 'happy'
// pairing with at least minBytes relayed — proof the payload traversed
// the Go relay and not some other path.
func requireHappyPairing(t *testing.T, s *stack, minBytes int64) {
	t.Helper()
	rows := usageRows(t, s.usageDB, 10*time.Second)
	for _, r := range rows {
		if r.result == "happy" && r.totalBytes >= minBytes {
			return
		}
	}
	t.Fatalf("expected a 'happy' pairing in the relay usage db with >= %d bytes relayed; rows: %v", minBytes, rows)
}

// ---------- Go -> Python (via the Go relay) ----------

func TestFileGoToPython(t *testing.T) {
	s := startStack(t)

	srcDir := t.TempDir()
	src := filepath.Join(srcDir, "payload.bin")
	want := make([]byte, 3*1024*1024+12345)
	if _, err := rand.Read(want); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, want, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	code, results, err := goClient(s).SendFile(ctx, "payload.bin", f)
	if err != nil {
		t.Fatalf("SendFile: %v", err)
	}

	outDir := t.TempDir()
	py := requirePython(t)
	cmd := pythonCmd(ctx, py, s.pyArgs("receive", "--no-listen", "--accept-file", code)...)
	cmd.Dir = outDir
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("python receive failed: %v\nstdout: %s\nstderr: %s", err, stdout.String(), stderr.String())
	}
	got := filepath.Join(outDir, "payload.bin")
	wantSum := sha256.Sum256(want)
	if gotSum := sha256File(t, got); gotSum != hex.EncodeToString(wantSum[:]) {
		t.Fatalf("file checksum mismatch for received payload.bin")
	}
	res := <-results
	if !res.OK {
		t.Fatalf("send result: %v", res.Error)
	}
	requireHappyPairing(t, s, int64(len(want)))
}

func TestDirectoryGoToPython(t *testing.T) {
	s := startStack(t)

	root, dirname, want := makeDirectory(t, "tree")
	var entries []wormhole.DirectoryEntry
	walkErr := filepath.Walk(filepath.Join(root, dirname), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !info.Mode().IsRegular() {
			return nil
		}
		relPath, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		entries = append(entries, wormhole.DirectoryEntry{
			Path: filepath.ToSlash(relPath),
			Mode: info.Mode(),
			Reader: func() (io.ReadCloser, error) {
				return os.Open(path)
			},
		})
		return nil
	})
	if walkErr != nil {
		t.Fatal(walkErr)
	}
	if len(entries) == 0 {
		t.Fatal("no entries collected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	code, results, err := goClient(s).SendDirectory(ctx, dirname, entries)
	if err != nil {
		t.Fatalf("SendDirectory: %v", err)
	}

	py := requirePython(t)
	outDir := t.TempDir()
	cmd := pythonCmd(ctx, py, s.pyArgs("receive", "--no-listen", "--accept-file", code)...)
	cmd.Dir = outDir
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("python receive failed: %v\nstdout: %s\nstderr: %s", err, stdout.String(), stderr.String())
	}
	got := walkChecksums(t, filepath.Join(outDir, dirname))
	compareTrees(t, want, got)
	res := <-results
	if !res.OK {
		t.Fatalf("send result: %v", res.Error)
	}
	requireHappyPairing(t, s, 1)
}

// ---------- Python -> Go (via the Go relay) ----------

func TestFilePythonToGo(t *testing.T) {
	s := startStack(t)

	srcDir := t.TempDir()
	src := filepath.Join(srcDir, "payload.bin")
	want := make([]byte, 3*1024*1024+12345)
	if _, err := rand.Read(want); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, want, 0o600); err != nil {
		t.Fatal(err)
	}
	code := "43-e2e-file"

	py := requirePython(t)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	sendCmd := pythonCmd(ctx, py, s.pyArgs("send", "--no-qr", "--no-listen", "--code", code, src)...)
	var sendOut bytes.Buffer
	sendCmd.Stdout, sendCmd.Stderr = &sendOut, &sendOut
	if err := sendCmd.Start(); err != nil {
		t.Fatal(err)
	}

	msg, err := goClient(s).Receive(ctx, code)
	if err != nil {
		_ = sendCmd.Process.Kill()
		_ = sendCmd.Wait()
		t.Fatalf("Receive: %v\npython output: %s", err, sendOut.String())
	}
	if msg.Type != wormhole.TransferFile {
		t.Fatalf("expected file transfer, got %v", msg.Type)
	}
	if msg.Name != "payload.bin" {
		t.Fatalf("offer file name: got %q, want payload.bin", msg.Name)
	}
	got, err := io.ReadAll(msg)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("size mismatch: got %d, want %d", len(got), len(want))
	}
	wantSum := sha256.Sum256(want)
	gotSum := sha256.Sum256(got)
	if wantSum != gotSum {
		t.Fatal("file checksum mismatch")
	}
	if err := sendCmd.Wait(); err != nil {
		t.Fatalf("python send failed: %v\noutput: %s", err, sendOut.String())
	}
	requireHappyPairing(t, s, int64(len(want)))
}

func TestDirectoryPythonToGo(t *testing.T) {
	s := startStack(t)

	root, dirname, want := makeDirectory(t, "tree")
	code := "44-e2e-dir"

	py := requirePython(t)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// cwd=root so the advertised name is the bare directory name
	sendCmd := pythonCmd(ctx, py, s.pyArgs("send", "--no-qr", "--no-listen", "--code", code, dirname)...)
	sendCmd.Dir = root
	var sendOut bytes.Buffer
	sendCmd.Stdout, sendCmd.Stderr = &sendOut, &sendOut
	if err := sendCmd.Start(); err != nil {
		t.Fatal(err)
	}

	msg, err := goClient(s).Receive(ctx, code)
	if err != nil {
		_ = sendCmd.Process.Kill()
		_ = sendCmd.Wait()
		t.Fatalf("Receive: %v\npython output: %s", err, sendOut.String())
	}
	if msg.Type != wormhole.TransferDirectory {
		t.Fatalf("expected directory transfer, got %v", msg.Type)
	}
	payload, err := io.ReadAll(msg)
	if err != nil {
		t.Fatal(err)
	}

	destRoot := t.TempDir()
	extractZipCanonical(t, payload, destRoot, dirname)
	got := walkChecksums(t, filepath.Join(destRoot, dirname))
	compareTrees(t, want, got)

	if err := sendCmd.Wait(); err != nil {
		t.Fatalf("python send failed: %v\noutput: %s", err, sendOut.String())
	}
	requireHappyPairing(t, s, 1)
}

// ---------- controls through the relay ----------

func TestFilePythonToPython(t *testing.T) {
	s := startStack(t)

	srcDir := t.TempDir()
	src := filepath.Join(srcDir, "control.bin")
	want := make([]byte, 1024*1024+7)
	if _, err := rand.Read(want); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, want, 0o600); err != nil {
		t.Fatal(err)
	}
	wantSum := sha256.Sum256(want)
	code := "45-e2e-ctl"

	py := requirePython(t)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	sendCmd := pythonCmd(ctx, py, s.pyArgs("send", "--no-qr", "--no-listen", "--code", code, src)...)
	var sendOut bytes.Buffer
	sendCmd.Stdout, sendCmd.Stderr = &sendOut, &sendOut
	if err := sendCmd.Start(); err != nil {
		t.Fatal(err)
	}

	outDir := t.TempDir()
	recvCmd := pythonCmd(ctx, py, s.pyArgs("receive", "--no-listen", "--accept-file", code)...)
	recvCmd.Dir = outDir
	var recvOut bytes.Buffer
	recvCmd.Stdout, recvCmd.Stderr = &recvOut, &recvOut
	if err := recvCmd.Run(); err != nil {
		_ = sendCmd.Process.Kill()
		_ = sendCmd.Wait()
		t.Fatalf("python receive failed: %v\noutput: %s", err, recvOut.String())
	}
	if err := sendCmd.Wait(); err != nil {
		t.Fatalf("python send failed: %v\noutput: %s", err, sendOut.String())
	}
	if gotSum := sha256File(t, filepath.Join(outDir, "control.bin")); gotSum != hex.EncodeToString(wantSum[:]) {
		t.Fatal("file checksum mismatch (python -> python control)")
	}
	requireHappyPairing(t, s, int64(len(want)))
}

func TestFileGoToGo(t *testing.T) {
	s := startStack(t)

	want := make([]byte, 2*1024*1024+99)
	if _, err := rand.Read(want); err != nil {
		t.Fatal(err)
	}
	src := bytes.NewReader(want)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	code, results, err := goClient(s).SendFile(ctx, "control.bin", src)
	if err != nil {
		t.Fatalf("SendFile: %v", err)
	}

	type receiverResult struct {
		msg *wormhole.IncomingMessage
		err error
	}
	goResult := make(chan receiverResult, 1)
	go func() {
		msg, err := goClient(s).Receive(ctx, code)
		goResult <- receiverResult{msg, err}
	}()

	rr := <-goResult
	if rr.err != nil {
		t.Fatalf("Receive: %v", rr.err)
	}
	if rr.msg.Type != wormhole.TransferFile {
		t.Fatalf("expected file transfer, got %v", rr.msg.Type)
	}
	got, err := io.ReadAll(rr.msg)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("file checksum mismatch (go -> go control)")
	}
	res := <-results
	if !res.OK {
		t.Fatalf("send result: %v", res.Error)
	}
	requireHappyPairing(t, s, int64(len(want)))
}

// ---------- helpers ----------

// extractZipCanonical unpacks a sender-produced zip into destRoot/dirname,
// stripping a single leading "<dirname>/" path component if present (Go
// senders produce entries relative to the directory; Python senders include
// the directory name). Paths that would escape destRoot are rejected.
func extractZipCanonical(t *testing.T, payload []byte, destRoot, dirname string) {
	t.Helper()
	zr, err := zip.NewReader(bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		t.Fatalf("payload is not a zip archive: %v", err)
	}
	prefix := dirname + "/"
	for _, zf := range zr.File {
		name := strings.TrimPrefix(filepath.ToSlash(zf.Name), "/")
		name = strings.TrimPrefix(name, prefix)
		if name == "" {
			continue // the directory entry itself
		}
		if strings.HasPrefix(name, "/") || name == ".." || strings.Contains(name, "/../") || strings.HasPrefix(name, "../") {
			t.Fatalf("unsafe path in zip: %q", zf.Name)
		}
		dest := filepath.Join(destRoot, dirname, filepath.FromSlash(name))
		absDest, err := filepath.Abs(dest)
		if err != nil {
			t.Fatal(err)
		}
		base, err := filepath.Abs(filepath.Join(destRoot, dirname))
		if err != nil {
			t.Fatal(err)
		}
		if absDest != base && !strings.HasPrefix(absDest, base+string(os.PathSeparator)) {
			t.Fatalf("path escapes target directory: %q", zf.Name)
		}
		if zf.FileInfo().IsDir() {
			if err := os.MkdirAll(dest, 0o755); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			t.Fatal(err)
		}
		r, err := zf.Open()
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(dest)
		if err != nil {
			r.Close()
			t.Fatal(err)
		}
		_, copyErr := io.Copy(f, r)
		r.Close()
		f.Close()
		if copyErr != nil {
			t.Fatal(copyErr)
		}
	}
}
