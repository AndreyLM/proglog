package log

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/andreylm/proglog/api/v1"

	"github.com/pkg/errors"
)

// Log - log
type Log struct {
	mu            *sync.RWMutex
	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}

// NewLog - new log
func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1 << 10
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1 << 10
	}
	l := &Log{
		Dir:    dir,
		Config: c,
		mu:     &sync.RWMutex{},
	}

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "read log dir: %s", dir)
	}
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return nil, errors.Wrap(err, "new segment")
		}
		i++
	}
	if l.segments == nil {
		if err = l.newSegment(c.Segment.InitialOffset); err != nil {
			return nil, errors.Wrap(err, "new segment")
		}
	}

	return l, nil
}

// Append - appends record to log
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, errors.Wrap(err, "append record to active segment")
	}
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

// Read - reads log
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	l.mu.RUnlock()
	for _, s := range l.segments {
		if off >= s.baseOffset && off < s.nextOffset {
			return s.Read(off)
		}
	}

	return nil, fmt.Errorf("offset out of range: %d", off)
}

// Close - closes log
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, s := range l.segments {
		if err := s.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Remove - removes log with all files
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return errors.Wrap(err, "close")
	}

	return os.RemoveAll(l.Dir)
}

// Reset - resets log
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return errors.Wrap(err, "remove")
	}

	newLog, err := NewLog(l.Dir, l.Config)
	if err != nil {
		return errors.Wrap(err, "new log")
	}
	*l = *newLog
	return nil
}

// LowestOffset - gets lowest offset
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

// HighestOffset - gets lowest offset
func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}

	return off - 1, nil
}

// Truncate - truncates log
func (l *Log) Truncate(lowerst uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowerst+1 {
			if err := s.Remove(); err != nil {
				return errors.Wrap(err, "remove segment")
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

// Reader - gets log reader
func (l *Log) Reader() io.Reader {
	l.mu.Lock()
	defer l.mu.Unlock()
	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, uint64(o.off))
	o.off += int64(n)
	return n, err
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return errors.Wrap(err, "new segment")
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}
