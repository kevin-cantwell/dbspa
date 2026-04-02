package source

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
)

// ExecSource runs a shell command and provides its stdout as a record source.
type ExecSource struct {
	cmd    *exec.Cmd
	stdout io.ReadCloser
	stderr io.ReadCloser
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

	// Forward subprocess stderr to FoldDB's stderr with prefix
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := stderr.Read(buf)
			if n > 0 {
				fmt.Fprintf(os.Stderr, "[exec] %s", buf[:n])
			}
			if err != nil {
				return
			}
		}
	}()

	return &ExecSource{cmd: cmd, stdout: stdout, stderr: stderr}, nil
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
		// Allow large lines (up to 10MB)
		scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			// Copy the bytes since scanner reuses the buffer
			cp := make([]byte, len(line))
			copy(cp, line)
			ch <- cp
		}
	}()
	return ch
}

// Wait waits for the command to finish and returns any exit error.
// A non-zero exit is returned as an error but callers may treat it as a warning
// since partial output from the command may still be valid.
func (e *ExecSource) Wait() error {
	return e.cmd.Wait()
}
