package oganesson

import (
	"io"
	"testing"

	"github.com/darkwyrm/oganesson/membufio"
)

func TestSegment1(t *testing.T) {
	var seg Segment
	seg.Type = DFStringType
	seg.Value = make([]byte, 9)
	if seg.GetSize() != 12 {
		t.Fatalf("Segment.Size: expected 12, got %d", seg.GetSize())
	}

	br := membufio.Make(10)
	br.Write([]byte("\x0e\x00\x04ABCD"))

	br.Seek(0, io.SeekStart)
	err := seg.Read(&br)
	if err != nil {
		t.Fatalf("Segment.Read failed: %s", err.Error())
	}
	if seg.Type != 14 || string(seg.Value) != "ABCD" {
		t.Fatalf("Segment.Read data mismatch: %d,%s", seg.Type, string(seg.Value))
	}

	// Wasteful, but a quick reset to all zeroes for a test. *shrug*
	br = membufio.Make(10)

	seg.Type = DFBinaryType
	seg.Value = []byte("EFGH")
	br.Seek(0, io.SeekStart)
	err = seg.Write(&br)
	if err != nil {
		t.Fatalf("Segment.Write failed: %s", err.Error())
	}
	if br.Buffer[0] != DFBinaryType || string(br.Buffer[3:7]) != "EFGH" {
		t.Fatalf("Segment.Write data mismatch: %d,%s", seg.Type, string(seg.Value))
	}
}

func TestSegment2(t *testing.T) {
	var seg Segment

	test32 := uint32(10)
	if err := seg.Set(test32); err != nil {
		t.Fatalf("Error setting uint32: %s", err.Error())
	}
	if seg.Value[3] != 10 {
		t.Fatalf("Set assigned wrong uint32 value: %d", seg.Value[3])
	}

	testFlag := true
	if err := seg.Set(testFlag); err != nil {
		t.Fatalf("Error setting bool: %s", err.Error())
	}
	if seg.Value[0] != 1 {
		t.Fatalf("Set assigned wrong bool value: %d", seg.Value[3])
	}

	testFlag, err := seg.GetBool()
	if err != nil {
		t.Fatalf("Error getting bool: %s", err.Error())
	}
	if !testFlag {
		t.Fatalf("get obtained wrong bool value: %v", testFlag)
	}
}

func TestFlatUnflatSegment(t *testing.T) {

	testBuffer, err := FlattenSegment(DFStringType, []byte("ABCDEFGH"))
	if err != nil {
		t.Fatalf("FlattenSegment failed: %s", err.Error())
	}
	if testBuffer == nil {
		t.Fatalf("FlattenSegment returned a nil buffer")
	}
	if testBuffer[0] != DFStringType {
		t.Fatalf("FlattenSegment type code failure: %d", testBuffer[0])
	}
	if testBuffer[2] != 8 {
		t.Fatalf("FlattenSegment size failure: %d", testBuffer[2])
	}
	if string(testBuffer[3:]) != "ABCDEFGH" {
		t.Fatalf("FlattenSegment data mismatch: %s", string(testBuffer[3:]))
	}

	testBuffer = []byte("\x0e\x00\x03ABC")
	seg, err := UnflattenSegment(testBuffer)
	if err != nil {
		t.Fatalf("UnflattenSegment failed: %s", err.Error())
	}
	if seg.Value == nil {
		t.Fatalf("UnflattenSegment returned an invalid Segment")
	}
	if seg.Type != DFStringType {
		t.Fatalf("UnflattenSegment type code failure: %d", seg.Type)
	}
	if len(seg.Value) != 3 {
		t.Fatalf("UnflattenSegment size failure: %d", len(seg.Value))
	}
	if string(seg.Value) != "ABC" {
		t.Fatalf("UnflattenSegment data mismatch: %s", string(seg.Value))
	}
}

func TestCountSegments(t *testing.T) {
	buffer := []byte("\x0e\x00\x03ABC\x0e\x00\x03DEF\x0e\x00\x03GHI")
	fieldCount, err := CountSegments(buffer)
	if err != nil {
		t.Fatalf("Error counting fields: %s", err.Error())
	}
	if fieldCount != 3 {
		t.Fatalf("CountSegments found %d items, expected %d", fieldCount, 3)
	}
}

func TestSegmentMap(t *testing.T) {
	fm := make(SegmentMap)
	// The map size field itself will occupy 3 bytes
	fm["test1"] = Segment{DFStringType, []byte("ABCDEF")}   // key size = 8, value size = 9, == 17
	fm["test2"] = Segment{DFBoolType, []byte("\x01")}       // key size = 8, value size = 2, == 10
	fm["test3"] = Segment{DFUInt16Type, []byte("\x0f\xff")} // key size = 8, value size = 3, == 11

	if _, ok := fm["test1"]; !ok {
		t.Fatalf("SegmentMapSize test1 missing")
	}
	totalSize := fm.GetSize()
	if totalSize != 41 {
		t.Fatalf("SegmentMapSize incorrect: wanted 41, got %d", totalSize)
	}

	bs := membufio.Make(totalSize)
	if err := fm.Write(&bs); err != nil {
		t.Fatalf("Error writing field map: %s", err.Error())
	}
	expectedData := []byte("\x12\x00\x03" +
		"\x0e\x00\x05\x74\x65\x73\x74\x31" + // key "test1"
		"\x0e\x00\x06\x41\x42\x43\x44\x45\x46" + // string value "ABCDEF"
		"\x0e\x00\x05\x74\x65\x73\x74\x32" + // key "test2"
		"\x0b\x01" + // boolean value True
		"\x0e\x00\x05\x74\x65\x73\x74\x33" + // key "test3"
		"\x06\x0f\xff") // uint16 value 4095

	if uint64(bs.BufferLength) != uint64(len(expectedData)) {
		t.Fatalf("WriteSegmentMap length mismatch. Got %d, expected %d", uint64(bs.BufferLength),
			len(expectedData))
	}

	fieldCount, err := CountSegments(bs.Buffer[:totalSize])
	if err != nil {
		t.Fatalf("Error counting fields: %s", err.Error())
	}
	if fieldCount != 7 {
		t.Fatalf("WriteSegmentMap wrote %d items, expected %d", fieldCount, 7)
	}

	fm.Clear()
	testBuffer := []byte(
		"\x12\x00\x02\x0e\x00\x04test\x0e\x00\x02AB\x0e\x00\x05test2\x03\x0a")
	bs = membufio.New(testBuffer)
	err = fm.Read(&bs)
	if err != nil {
		t.Fatalf("Error reading fields: %s", err.Error())
	}
	if len(fm) != 2 {
		t.Fatalf("ReadSegment read %d items, expected %d", len(fm), 2)
	}
}

func TestSegmentList(t *testing.T) {
	sl := make(SegmentList, 3)
	// The list size field itself will occupy 3 bytes
	sl[0] = Segment{DFStringType, []byte("ABCDEF")}   // item size = 9, == 12
	sl[1] = Segment{DFBoolType, []byte("\x01")}       // item size = 2, == 14
	sl[2] = Segment{DFUInt16Type, []byte("\x0f\xff")} // item size = 3, == 17

	totalSize := sl.GetSize()
	if totalSize != 17 {
		t.Fatalf("SegmentListSize incorrect: wanted 17, got %d", totalSize)
	}

	bs := membufio.Make(totalSize)
	if err := sl.Write(&bs); err != nil {
		t.Fatalf("Error writing field list: %s", err.Error())
	}
	expectedData := []byte("\x13\x00\x03" +
		"\x0e\x00\x06\x41\x42\x43\x44\x45\x46" + // string value "ABCDEF"
		"\x0b\x01" + // boolean value True
		"\x06\x0f\xff") // uint16 value 4095

	if uint64(bs.BufferLength) != uint64(len(expectedData)) {
		t.Fatalf("WriteSegmentList length mismatch. Got %d, expected %d", uint64(bs.BufferLength),
			len(expectedData))
	}

	for i, v := range expectedData {
		if v != bs.Buffer[i] {
			t.Fatalf("WriteSegmentList data mismatch at index %d", i)
		}
	}

	fieldCount, err := CountSegments(bs.Buffer)
	if err != nil {
		t.Fatalf("Error counting fields: %s", err.Error())
	}
	if fieldCount != 4 {
		t.Fatalf("WriteSegmentList wrote %d items, expected %d", fieldCount, 7)
	}

	sl.Clear()
	testBuffer := []byte(
		"\x13\x00\x04\x0e\x00\x04test\x0e\x00\x02AB\x0e\x00\x05test2\x03\x0a")
	err = sl.Read(testBuffer)
	if err != nil {
		t.Fatalf("Error reading fields: %s", err.Error())
	}
	if len(sl) != 4 {
		t.Fatalf("ReadSegment read %d items, expected %d", len(sl), 4)
	}
}

func TestGetSet(t *testing.T) {

	var seg Segment

	// Test Document Start/End

	if err := seg.SetDocStart(1); err != nil {
		t.Fatalf("TestGetSet failed to set document start: %s", err.Error())
	}
	testu8, err := seg.GetDocStart()
	if err != nil {
		t.Fatalf("TestGetSet failed to get document start: %s", err.Error())
	}
	if testu8 != 1 {
		t.Fatalf("TestGetSet value failure wanted 1, got %v", testu8)
	}

	if err := seg.SetDocEnd(9); err != nil {
		t.Fatalf("TestGetSet failed to set document end: %s", err.Error())
	}
	testu64, err := seg.GetDocEnd()
	if err != nil {
		t.Fatalf("TestGetSet failed to get document end: %s", err.Error())
	}
	if testu64 != 9 {
		t.Fatalf("TestGetSet value failure wanted 9, got %v", testu64)
	}

	// Integer type tests

	if err := seg.SetInt8(10); err != nil {
		t.Fatalf("TestGetSet failed to set an int8: %s", err.Error())
	}
	tests8, err := seg.GetInt8()
	if err != nil {
		t.Fatalf("TestGetSet failed to get an int8: %s", err.Error())
	}
	if tests8 != 10 {
		t.Fatalf("TestGetSet value failure wanted 10, got %v", tests8)
	}

	if err := seg.SetUInt8(15); err != nil {
		t.Fatalf("TestGetSet failed to set a uint8: %s", err.Error())
	}
	testu8, err = seg.GetUInt8()
	if err != nil {
		t.Fatalf("TestGetSet failed to get a uint8: %s", err.Error())
	}
	if testu8 != 15 {
		t.Fatalf("TestGetSet value failure wanted 15, got %v", testu8)
	}

	if err := seg.SetInt16(300); err != nil {
		t.Fatalf("TestGetSet failed to set an int16: %s", err.Error())
	}
	tests16, err := seg.GetInt16()
	if err != nil {
		t.Fatalf("TestGetSet failed to get an int16: %s", err.Error())
	}
	if tests16 != 300 {
		t.Fatalf("TestGetSet value failure wanted 300, got %v", tests16)
	}

	if err := seg.SetUInt16(400); err != nil {
		t.Fatalf("TestGetSet failed to set a uint16: %s", err.Error())
	}
	testu16, err := seg.GetUInt16()
	if err != nil {
		t.Fatalf("TestGetSet failed to get a uint16: %s", err.Error())
	}
	if testu16 != 400 {
		t.Fatalf("TestGetSet value failure wanted 400, got %v", testu16)
	}

	if err := seg.SetInt32(70000); err != nil {
		t.Fatalf("TestGetSet failed to set an int32: %s", err.Error())
	}
	tests32, err := seg.GetInt32()
	if err != nil {
		t.Fatalf("TestGetSet failed to get an int32: %s", err.Error())
	}
	if tests32 != 70000 {
		t.Fatalf("TestGetSet value failure wanted 70000, got %v", tests32)
	}

	if err := seg.SetUInt32(71000); err != nil {
		t.Fatalf("TestGetSet failed to set a uint32: %s", err.Error())
	}
	testu32, err := seg.GetUInt32()
	if err != nil {
		t.Fatalf("TestGetSet failed to get a uint32: %s", err.Error())
	}
	if testu32 != 71000 {
		t.Fatalf("TestGetSet value failure wanted 71000, got %v", testu32)
	}

	if err := seg.SetInt64(0x10000); err != nil {
		t.Fatalf("TestGetSet failed to set an int64: %s", err.Error())
	}
	tests64, err := seg.GetInt64()
	if err != nil {
		t.Fatalf("TestGetSet failed to get an int64: %s", err.Error())
	}
	if tests64 != 0x10000 {
		t.Fatalf("TestGetSet value failure wanted 0x10000, got %x", tests64)
	}

	if err := seg.SetUInt64(0x20000); err != nil {
		t.Fatalf("TestGetSet failed to set a uint64: %s", err.Error())
	}
	testu64, err = seg.GetUInt64()
	if err != nil {
		t.Fatalf("TestGetSet failed to get a uint64: %s", err.Error())
	}
	if testu64 != 0x20000 {
		t.Fatalf("TestGetSet value failure wanted 0x20000, got %x", testu64)
	}

	// Container index type tests

	testMap := make(SegmentMap, 2)
	testMap["start"] = Segment{DFDocumentStart, []byte{1}}
	testMap["test8"] = Segment{DFUInt8Type, []byte{2}}
	if err := seg.SetMapIndex(testMap); err != nil {
		t.Fatalf("TestGetSet failed to set a map index value: %s", err.Error())
	}
	mapSize, err := seg.GetMapIndex()
	if err != nil {
		t.Fatalf("TestGetSet failed to get a map index value: %s", err.Error())
	}
	if mapSize != 2 {
		t.Fatalf("TestGetSet map index value failure wanted 2, got %v", mapSize)
	}

	testList := make(SegmentList, 0)
	testList = append(testList, Segment{DFDocumentStart, []byte{1}})
	testList = append(testList, Segment{DFUInt8Type, []byte{2}})
	testList = append(testList, Segment{DFUInt16Type, []byte{2, 2}})
	if err := seg.SetListIndex(testList); err != nil {
		t.Fatalf("TestGetSet failed to set a list index value: %s", err.Error())
	}
	listSize, err := seg.GetListIndex()
	if err != nil {
		t.Fatalf("TestGetSet failed to get a list index value: %s", err.Error())
	}
	if listSize != 3 {
		t.Fatalf("TestGetSet list index value failure wanted 3, got %v", listSize)
	}

	// Tests for other data types

	if err := seg.SetBool(true); err != nil {
		t.Fatalf("TestGetSet failed to set a bool: %s", err.Error())
	}
	testbool, err := seg.GetBool()
	if err != nil {
		t.Fatalf("TestGetSet failed to get a bool: %s", err.Error())
	}
	if !testbool {
		t.Fatalf("TestGetSet value failure wanted true, got false")
	}

	if err := seg.SetFloat32(1.1); err != nil {
		t.Fatalf("TestGetSet failed to set a float32: %s", err.Error())
	}
	testf32, err := seg.GetFloat32()
	if err != nil {
		t.Fatalf("TestGetSet failed to get a float32: %s", err.Error())
	}
	if testf32 != 1.1 {
		t.Fatalf("TestGetSet value failure wanted 1.1, got %v", testf32)
	}

	if err := seg.SetFloat64(2.2); err != nil {
		t.Fatalf("TestGetSet failed to set a float64: %s", err.Error())
	}
	testf64, err := seg.GetFloat64()
	if err != nil {
		t.Fatalf("TestGetSet failed to get a float64: %s", err.Error())
	}
	if testf64 != 2.2 {
		t.Fatalf("TestGetSet value failure wanted 2.2, got %v", testf64)
	}

	if err := seg.SetString("This is a test string"); err != nil {
		t.Fatalf("TestGetSet failed to set a string: %s", err.Error())
	}
	teststr, err := seg.GetString()
	if err != nil {
		t.Fatalf("TestGetSet failed to get a string: %s", err.Error())
	}
	if teststr != "This is a test string" {
		t.Fatalf("TestGetSet value failure wanted 'This is a test string', got '%v'", teststr)
	}

	if err := seg.SetBinary([]byte("This is some test binary data")); err != nil {
		t.Fatalf("TestGetSet failed to set a string: %s", err.Error())
	}
	testbin, err := seg.GetBinary()
	if err != nil {
		t.Fatalf("TestGetSet failed to get binary data: %s", err.Error())
	}
	if string(testbin) != "This is some test binary data" {
		t.Fatalf("TestGetSet value failure wanted b'This is some test binary data', got b'%v'",
			testbin)
	}

}
