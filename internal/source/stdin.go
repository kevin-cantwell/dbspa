package source

import (
	"bufio"
	"io"
)

// Stdin reads lines from an io.Reader (typically os.Stdin) and sends each
// non-empty line as a raw byte slice.
type Stdin struct {
	Reader io.Reader
}

// Read starts reading lines from stdin and returns a channel of raw bytes.
func (s *Stdin) Read() <-chan []byte {
	ch := make(chan []byte, 1024)
	go func() {
		defer close(ch)
		scanner := bufio.NewScanner(s.Reader)
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
