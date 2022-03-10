package membufio

import (
	"io"
	"testing"
)

func TestByteSliceIO(t *testing.T) {

	storage := make([]byte, 20)

	bs := New(storage)
	if bs.Index != 0 || bs.BufferLength != int64(len(storage)) {
		t.Fatal("new() didn't init correctly")
	}

	// Write-related operations

	_, err := bs.WriteAt(nil, 0)
	if err != ErrEmptyData {
		t.Fatal("writeat() didn't handle nil buffer")
	}

	bw, err := bs.Write([]byte("AAAAAAAAAABBBBBBBBBB"))
	if err != nil {
		t.Fatalf("write() error: %s", err.Error())
	}
	if bw != 20 {
		t.Fatalf("write() only wrote %d instead of 20", bw)
	}
	if bs.Index != 20 {
		t.Fatalf("write() didn't update buffer index correctly")
	}
	if string(bs.Buffer) != "AAAAAAAAAABBBBBBBBBB" {
		t.Fatalf("incorrect data written in write(): %s", string(bs.Buffer))
	}

	// Read-related operations

	target := make([]byte, 10)
	br, err := bs.ReadAt(target, 5)

	if err != nil {
		t.Fatalf("readat() error: %s", err.Error())
	}
	if br != 10 {
		t.Fatalf("readat() read %d bytes instead of 10", bw)
	}
	if bs.Index != 20 {
		t.Fatalf("readat() changed the buffer index (and shouldn't have)")
	}
	if string(target) != "AAAAABBBBB" {
		t.Fatalf("incorrect data received from readat(): %s", string(target))
	}

	// Seek

	pos, err := bs.Seek(5, io.SeekStart)
	if err != nil {
		t.Fatalf("seek() subtest #1 error: %s", err.Error())
	}
	if pos != 5 {
		t.Fatalf("seek() subtest #1 resulted in wrong index: %d", pos)
	}

	pos, err = bs.Seek(5, io.SeekCurrent)
	if err != nil {
		t.Fatalf("seek() subtest #2 error: %s", err.Error())
	}
	if pos != 10 {
		t.Fatalf("seek() subtest #2 resulted in wrong index: %d", pos)
	}

	pos, err = bs.Seek(5, io.SeekEnd)
	if err != nil {
		t.Fatalf("seek() subtest #3 error: %s", err.Error())
	}
	if pos != 15 {
		t.Fatalf("seek() subtest #3 resulted in wrong index: %d", pos)
	}
}

func TestByteSliceIO2(t *testing.T) {

	bs := Make(20)

	bw, err := bs.WriteAt([]byte("DDDDDDDDDD--"), 10)
	if err != nil {
		t.Fatalf("writeat() #1 error: %s", err.Error())
	}
	if bw != 10 {
		t.Fatalf("writeat() #1 only wrote %d instead of 10", bw)
	}
	if bs.Index != 0 {
		t.Fatalf("writeat() #1 didn't update buffer index correctly")
	}

	bw, err = bs.WriteAt([]byte("CCCCCCCCCC"), 0)
	if err != nil {
		t.Fatalf("writeat() #2 error: %s", err.Error())
	}
	if bw != 10 {
		t.Fatalf("writeat() #2 only wrote %d instead of 10", bw)
	}

	if string(bs.Buffer) != "CCCCCCCCCCDDDDDDDDDD" {
		t.Fatalf("incorrect data written by writeat(): %s", string(bs.Buffer))
	}

	// At this point, index should still be 0

	for _, val := range []byte{1, 0, 17} {
		bs.WriteByte(val)
	}
	if bs.Index != 3 {
		t.Fatalf("writebyte didn't update the index properly. At %d, should be 3", bs.Index)
	}
	if bs.Buffer[0] != 1 || bs.Buffer[1] != 0 || bs.Buffer[2] != 17 {
		t.Fatalf("writebyte didn't write values correctly")
	}

	b, _ := bs.ReadByte()
	if b != byte('C') {
		t.Fatalf("readbyte failed to read character properly")
	}
	if bs.Index != 4 {
		t.Fatalf("readbyte didn't update the index properly. At %d, should be 4", bs.Index)
	}
}
