package membufio

import (
	"errors"
	"io"
)

var ErrEmptyData = errors.New("empty data")
var ErrRange = errors.New("index out of range")

// This package implements an IO interface to perform stream operations on a byte slice

type ByteSliceIO struct {
	Buffer       []byte
	Index        int64
	BufferLength int64
}

// New creates a new ByteSliceIO instance pointed at the passed byte slice
func New(p []byte) ByteSliceIO {
	var out ByteSliceIO
	out.Open(p)
	return out
}

// Make creates a new ByteSliceIO and allocates a memory buffer of the requested size
func Make(bufsize uint64) ByteSliceIO {
	p := make([]byte, bufsize)
	var out ByteSliceIO
	out.Open(p)
	return out
}

// Open points the ByteSliceIO instance at a new byte slice. It does not take ownership of the data.
func (bs *ByteSliceIO) Open(p []byte) error {
	targetLength := int64(len(p))
	if targetLength == 0 {
		return ErrEmptyData
	}

	bs.Buffer = p
	bs.BufferLength = targetLength
	bs.Index = 0

	return nil
}

// Close detaches from the specified byte slice and returns to an uninitialized state
func (bs *ByteSliceIO) Close() {
	bs.Buffer = nil
	bs.BufferLength = 0
	bs.Index = 0
}

func (bs *ByteSliceIO) IsEOF() bool {
	return bs.Index >= bs.BufferLength
}

// Read copies data from the source buffer into another byte slice from the current file position.
// It returns ErrEmptyData if given a target with no capacity (zero length or nil).
func (bs *ByteSliceIO) Read(p []byte) (int, error) {

	bytesRead, err := bs.ReadAt(p, bs.Index)
	if err == nil {
		bs.Index += int64(bytesRead)

		if bs.Index > bs.BufferLength {
			bs.Index = bs.BufferLength
			return bytesRead, errors.New("EOF")
		}
	}

	return bytesRead, err
}

// ReadAt copies data from the source buffer into another byte slice from the specified offset.
// It returns ErrEmptyData if given a target with no capacity (zero length or nil).
func (bs *ByteSliceIO) ReadAt(p []byte, offset int64) (int, error) {

	targetLength := int64(len(p))
	if targetLength == 0 {
		return 0, ErrEmptyData
	}

	bytesToRead := bs.BufferLength - offset
	if bytesToRead <= 0 {
		return 0, errors.New("EOF")
	}
	if bytesToRead > targetLength {
		bytesToRead = targetLength
	}

	bytesRead := copy(p, bs.Buffer[offset:offset+bytesToRead])
	return bytesRead, nil
}

// ReadByte reads a byte from the source buffer and returns it.
func (bs *ByteSliceIO) ReadByte() (byte, error) {

	if bs.Index >= bs.BufferLength {
		return 0, io.EOF
	}

	out := bs.Buffer[bs.Index]
	bs.Index++
	return out, nil
}

// Write copies data from the ByteSliceIO instance into another byte slice from the current file
// position. It returns ErrEmptyData if given a target with no capacity (zero length or nil).
func (bs *ByteSliceIO) Write(p []byte) (int, error) {

	bytesWritten, err := bs.WriteAt(p, bs.Index)
	if err == nil {
		bs.Index += int64(bytesWritten)

		if bs.Index > bs.BufferLength {
			bs.Index = bs.BufferLength
		}
	}
	return bytesWritten, err
}

// Write copies data from the ByteSliceIO instance into another byte slice from the specified
// offset. It returns ErrEmptyData if given a target with no capacity (zero length or nil).
func (bs *ByteSliceIO) WriteAt(p []byte, offset int64) (int, error) {

	sourceLength := int64(len(p))
	if len(p) == 0 {
		return 0, ErrEmptyData
	}

	bytesToWrite := bs.BufferLength - offset
	if bytesToWrite <= 0 {
		return 0, nil
	}
	if bytesToWrite > sourceLength {
		bytesToWrite = sourceLength
	}

	bytesWritten := copy(bs.Buffer[offset:offset+bytesToWrite], p[:bytesToWrite])
	return bytesWritten, nil
}

// WriteByte writes a byte to the source buffer.
func (bs *ByteSliceIO) WriteByte(value byte) error {

	if bs.Index >= bs.BufferLength {
		return io.EOF
	}

	bs.Buffer[bs.Index] = value
	bs.Index++
	return nil
}

// Seek jumps to the specified offset relative to the `whence` specifier, which can be io.SeekStart,
// io.SeekEnd, or io.SeekCurrent.
func (bs *ByteSliceIO) Seek(offset int64, whence int) (int64, error) {

	var start int64
	switch whence {
	case io.SeekStart:
		break
	case io.SeekEnd:
		start = bs.BufferLength
		offset *= -1
	case io.SeekCurrent:
		start = bs.Index
	default:
		return 0, errors.New("invalid whence value")
	}

	if start+offset < 0 {
		return 0, ErrRange
	}

	bs.Index = start + offset

	if bs.Index >= bs.BufferLength {
		return bs.Index, errors.New("EOF")
	}

	return bs.Index, nil
}
