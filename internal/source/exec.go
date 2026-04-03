package source

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"sync"
)

// ExecSource runs a shell command and provides its stdout as a record source.
// Stderr is buffered silently and only surfaced if the command exits non-zero.
type ExecSource struct {
	cmd    *exec.Cmd
	stdout io.ReadCloser

	stderrBuf bytes.Buffer // buffered stderr (last N bytes)
	stderrMu  sync.Mutex
	stderrMax int // max bytes to retain (default 4KB)
}

// NewExecSource creates an ExecSource. The command runs through /bin/sh -c.
// Context cancellation kills the subprocess via exec.CommandContext.
func NewExecSource(ctx context.Context, command string) (*ExecSource, error) {
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", command)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("exec stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("exec stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("exec start: %w", err)
	}

	es := &ExecSource{
		cmd:       cmd,
		stdout:    stdout,
		stderrMax: 4096,
	}

	// Buffer subprocess stderr silently — only surfaced on non-zero exit
	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := stderr.Read(buf)
			if n > 0 {
				es.stderrMu.Lock()
				es.stderrBuf.Write(buf[:n])
				// Trim to last stderrMax bytes if over limit
				if es.stderrBuf.Len() > es.stderrMax {
					b := es.stderrBuf.Bytes()
					es.stderrBuf.Reset()
					es.stderrBuf.Write(b[len(b)-es.stderrMax:])
				}
				es.stderrMu.Unlock()
			}
			if err != nil {
				return
			}
		}
	}()

	return es, nil
}

// ReadRaw returns the stdout reader for binary stream decoders.
func (e *ExecSource) ReadRaw() io.Reader {
	return e.stdout
}

// Read returns a channel of lines from stdout (for line-based decoders).
func (e *ExecSource) Read() <-chan []byte {
	ch := make(chan []byte, 1024)
	go func() {
		defer close(ch)
		scanner := bufio.NewScanner(e.stdout)
		scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			cp := make([]byte, len(line))
			copy(cp, line)
			ch <- cp
		}
	}()
	return ch
}

// Wait waits for the command to finish. Returns nil on success (exit 0).
// On non-zero exit, returns an error including the tail of stderr.
func (e *ExecSource) Wait() error {
	err := e.cmd.Wait()
	if err != nil {
		e.stderrMu.Lock()
		stderrTail := e.stderrBuf.String()
		e.stderrMu.Unlock()
		if stderrTail != "" {
			return fmt.Errorf("command failed (%w):\n%s", err, stderrTail)
		}
		return fmt.Errorf("command failed: %w", err)
	}
	return nil
}

// Stderr returns the buffered stderr output (up to 4KB of the most recent output).
func (e *ExecSource) Stderr() string {
	e.stderrMu.Lock()
	defer e.stderrMu.Unlock()
	return e.stderrBuf.String()
}
