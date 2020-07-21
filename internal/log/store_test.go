package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestStore(t *testing.T) {
	f, err := ioutil.TempFile("", "store_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)
	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, i*width)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(0); i < 3; i++ {
		msg, err := s.Read(i * width)
		require.NoError(t, err)
		require.Equal(t, msg, write)
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), uint64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, n, lenWidth)
		off += uint64(n)

		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, b, write)
		require.Equal(t, int(size), n)
		off += uint64(n)
	}
}
