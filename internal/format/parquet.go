package format

import (
	"fmt"
	"io"
	"math"

	"github.com/kevin-cantwell/folddb/internal/engine"
	"github.com/parquet-go/parquet-go"
)

// ParquetDecoder reads Parquet files and emits records.
// Implements StreamDecoder since Parquet is not line-delimited.
type ParquetDecoder struct{}

// Decode is not supported for Parquet (it's a file format, not line-based).
// This stub satisfies the Decoder interface for factory compatibility.
func (d *ParquetDecoder) Decode(data []byte) (engine.Record, error) {
	return engine.Record{}, fmt.Errorf("parquet format requires --input <file> (cannot decode individual lines)")
}

// DecodeStream reads a Parquet file from the reader and sends records to the channel.
// Note: parquet-go requires ReadSeeker, so for stdin we'd need to buffer.
// For --input files, os.File implements ReadSeeker natively.
func (d *ParquetDecoder) DecodeStream(r io.Reader, ch chan<- engine.Record) error {
	defer close(ch)

	// parquet.OpenFile needs a ReaderAt + size.
	// If we have a file (which we do via --input), use it directly.
	ra, ok := r.(io.ReaderAt)
	if !ok {
		return fmt.Errorf("parquet format requires --input <file> (cannot read parquet from stdin)")
	}

	// Get file size
	type sizer interface {
		Stat() (interface{ Size() int64 }, error)
	}
	// Try os.File first
	type statter interface {
		Stat() (interface {
			Size() int64
		}, error)
	}

	var size int64
	if f, ok := r.(interface{ Stat() (interface{ Size() int64 }, error) }); ok {
		stat, err := f.Stat()
		if err != nil {
			return fmt.Errorf("cannot stat parquet file: %w", err)
		}
		size = stat.Size()
	} else {
		// Try seeking to end to get size
		if seeker, ok := r.(io.Seeker); ok {
			var err error
			size, err = seeker.Seek(0, io.SeekEnd)
			if err != nil {
				return fmt.Errorf("cannot determine parquet file size: %w", err)
			}
			if _, err := seeker.Seek(0, io.SeekStart); err != nil {
				return fmt.Errorf("cannot seek parquet file: %w", err)
			}
		} else {
			return fmt.Errorf("parquet format requires a seekable file (use --input)")
		}
	}

	pf, err := parquet.OpenFile(ra, size)
	if err != nil {
		return fmt.Errorf("parquet open error: %w", err)
	}

	schema := pf.Schema()
	fields := schema.Fields()
	fieldNames := make([]string, len(fields))
	for i, f := range fields {
		fieldNames[i] = f.Name()
	}

	for _, rg := range pf.RowGroups() {
		rows := rg.Rows()
		rowBuf := make([]parquet.Row, 128)
		for {
			n, err := rows.ReadRows(rowBuf)
			for i := 0; i < n; i++ {
				rec := rowToRecord(rowBuf[i], fieldNames, fields)
				ch <- rec
			}
			if err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("parquet read error: %w", err)
			}
		}
		rows.Close()
	}

	return nil
}

func rowToRecord(row parquet.Row, names []string, fields []parquet.Field) engine.Record {
	cols := make(map[string]engine.Value, len(names))
	for i, val := range row {
		if i >= len(names) {
			break
		}
		cols[names[i]] = parquetValueToEngine(val)
	}
	return engine.Record{
		Columns: cols,
		Weight:    1,
	}
}

func parquetValueToEngine(v parquet.Value) engine.Value {
	if v.IsNull() {
		return engine.NullValue{}
	}
	switch v.Kind() {
	case parquet.Boolean:
		return engine.BoolValue{V: v.Boolean()}
	case parquet.Int32:
		return engine.IntValue{V: int64(v.Int32())}
	case parquet.Int64:
		return engine.IntValue{V: v.Int64()}
	case parquet.Float:
		return engine.FloatValue{V: float64(v.Float())}
	case parquet.Double:
		f := v.Double()
		if f == math.Trunc(f) && f >= math.MinInt64 && f <= math.MaxInt64 {
			return engine.IntValue{V: int64(f)}
		}
		return engine.FloatValue{V: f}
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return engine.TextValue{V: string(v.ByteArray())}
	default:
		return engine.TextValue{V: v.String()}
	}
}
