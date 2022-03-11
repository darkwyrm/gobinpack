package oganesson

import (
	"net"
	"testing"
	"time"

	"github.com/darkwyrm/oganesson/membufio"
)

func TestDocumentFlattenUnflattenSize(t *testing.T) {
	wm := NewDocument()
	wm.AttachString("testString", "abcdef")
	wm.AttachInt64("testInt", 42)

	// DocStart = 2
	// Message Code = 10
	// Map Count = 3
	// Attachments:
	//	Key testString = 13
	//	Value "abcdef" = 9
	//	Key testInt = 10
	//	Value 42 = 9
	// DocEnd = 9
	// Total = 65
	if wm.GetSize() != 65 {
		t.Fatalf("GetSize mismatch. Wanted 65, got %v\n", wm.GetSize())
	}

	p, err := wm.Flatten()
	if err != nil {
		t.Fatalf("Error flattening message: %s\n", err.Error())
	}

	expectedBytes := []byte(
		// DocStart
		"\x01\x01" +

			// String "TestMsg"
			"\x0e\x00\x07TestMsg" +

			// Map Count: 2
			"\x12\x00\x02" +

			// Map Key "testString"
			"\x0e\x00\x0atestString" +

			// Map Value "abcdef"
			"\x0e\x00\x06abcdef" +

			// Map Key "testInt"
			"\x0e\x00\x07testInt" +

			// Map Value int64 = 42
			"\x09\x00\x00\x00\x00\x00\x00\x00\x2a" +

			// DocEnd
			"\x02\x00\x00\x00\x00\x00\x00\x00\x06")

	if string(p) != string(expectedBytes) {
		t.Fatalf("Flatten output didn't match expected: \nexpected: % x\ngot:      % x\n",
			[]byte(expectedBytes), []byte(p))
	}

	var um Document
	err = um.Unflatten(expectedBytes)
	if err != nil {
		t.Fatalf("Error unflattening message: %s\n", err.Error())
	}
	msgCode, err := um.Segments[0].GetString()
	if err != nil {
		t.Fatalf("Error getting message code: %s", err.Error())
	}
	if msgCode != "TestMsg" {
		t.Fatalf("Wrong message code in unflattened message: expected 'TestMsg', got '%s'\n",
			msgCode)
	}
	if !um.Segments[1].Has("testString") {
		t.Fatalf("Missing field 'testString' in unflattened message\n")
	}
	if !um.Has("testInt") {
		t.Fatalf("Missing field 'testInt' in unflattened message\n")
	}

	tempString, err := um.GetString("testString")
	if err != nil {
		t.Fatalf("Error getting attachment 'testString': %s", err.Error())
	}
	if tempString != "abcdef" {
		t.Fatalf("Segment 'testString' had bad value: expected 'abcdef', got '%s'\n", tempString)
	}

	tempInt, err := um.GetInt64("testInt")
	if err != nil {
		t.Fatalf("Error getting field 'testInt' in unflattened message: %s\n", err.Error())
	}
	if tempInt != 42 {
		t.Fatalf("Segment 'testInt' had bad value: expected 42, got %v\n", tempInt)
	}
}

func DocumentReadWriteSetup(sync chan int, port string) {
	// Wait until the test is ready and then go from there
	<-sync
	time.Sleep(time.Millisecond * 100)

	senderconn, err := net.Dial("tcp", "127.0.0.1:"+port)
	if err != nil {
		return
	}
	s := NewPacketRequester(senderconn)
	s.Timeout = time.Minute * 5

	wm := NewDocument("TestMsg")
	wm.AttachString("testString", "abcdef")
	wm.AttachInt64("testInt", 42)

	bs := membufio.Make(128000)
	err = wm.Write(&bs)
	if err != nil {
		panic(err)
	}

	s.UpdateTimeout()
	err = wm.Write(s.Connection)

	if err != nil {
		panic(err)
	}
}

func TestDocumentReadWrite(t *testing.T) {
	MaxCommandLength = 300
	sync := make(chan int)
	go DocumentReadWriteSetup(sync, "3008")

	listener, err := net.Listen("tcp", "127.0.0.1:3008")
	if err != nil {
		t.Fatalf("Error setting up listener: %s", err.Error())
	}
	defer listener.Close()

	sync <- 1
	conn, err := listener.Accept()
	if err != nil {
		t.Fatalf("Error accepting a connection: %s", err.Error())
	}
	defer conn.Close()

	s := NewPacketResponder(conn, 32767)
	s.Timeout = time.Minute * 5

	wm := NewDocument("")
	s.UpdateTimeout()
	err = wm.Read(s.Connection)
	if err != nil {
		t.Fatalf("Error receiving wire message: %s", err.Error())
	}

	if wm.MsgCode != "TestMsg" {
		t.Fatalf("Incorrect wire message code received: expected 'TestMsg', got '%s'", wm.MsgCode)
	}

	if !wm.Has("testString") {
		t.Fatal("Message missing field testString")
	}
	_, err = wm.GetString("testString")
	if err != nil {
		t.Fatalf("Error getting string field 'testString': %s", err.Error())
	}

	if !wm.Has("testInt") {
		t.Fatal("Message missing field testString")
	}
	_, err = wm.GetInt64("testInt")
	if err != nil {
		t.Fatalf("Error getting int field 'testInt': %s", err.Error())
	}
}

func TestDocumentGetSetInt(t *testing.T) {

	wm := NewDocument("TestCommand")

	if err := wm.AttachInt8("testval", 3); err != nil {
		t.Fatalf("TestDocumentGetSetInt failed to set an int8: %s", err.Error())
	}
	tests8, err := wm.GetInt8("testval")
	if err != nil {
		t.Fatalf("TestDocumentGetSetInt failed to get an int8: %s", err.Error())
	}
	if tests8 != 3 {
		t.Fatalf("TestDocumentGetSetInt int8 value failure: wanted 3, got %v", tests8)
	}

	if err := wm.AttachUInt8("testval", 4); err != nil {
		t.Fatalf("TestDocumentGetSetInt failed to set a uint8: %s", err.Error())
	}
	testu8, err := wm.GetUInt8("testval")
	if err != nil {
		t.Fatalf("TestDocumentGetSetInt failed to get a uint8: %s", err.Error())
	}
	if testu8 != 4 {
		t.Fatalf("TestDocumentGetSetInt uint8 value failure: wanted 4, got %v", testu8)
	}

	if err := wm.AttachInt16("testval", 1000); err != nil {
		t.Fatalf("TestDocumentGetSetInt failed to set an int16: %s", err.Error())
	}
	tests16, err := wm.GetInt16("testval")
	if err != nil {
		t.Fatalf("TestDocumentGetSetInt failed to get an int16: %s", err.Error())
	}
	if tests16 != 1000 {
		t.Fatalf("TestDocumentGetSetInt int16 value failure: wanted 1000, got %v", tests16)
	}

	if err := wm.AttachUInt16("testval", 2000); err != nil {
		t.Fatalf("TestDocumentGetSetInt failed to set a uint16: %s", err.Error())
	}
	testu16, err := wm.GetUInt16("testval")
	if err != nil {
		t.Fatalf("TestDocumentGetSetInt failed to get a uint16: %s", err.Error())
	}
	if testu16 != 2000 {
		t.Fatalf("TestDocumentGetSetInt uint16 value failure: wanted 2000, got %v", testu16)
	}

	if err := wm.AttachInt32("testval", 70000); err != nil {
		t.Fatalf("TestDocumentGetSetInt failed to set an int32: %s", err.Error())
	}
	tests32, err := wm.GetInt32("testval")
	if err != nil {
		t.Fatalf("TestDocumentGetSetInt failed to get an int32: %s", err.Error())
	}
	if tests32 != 70000 {
		t.Fatalf("TestDocumentGetSetInt int32 value failure: wanted 70000, got %v", tests32)
	}

	if err := wm.AttachUInt32("testval", 80000); err != nil {
		t.Fatalf("TestDocumentGetSetInt failed to set a uint32: %s", err.Error())
	}
	testu32, err := wm.GetUInt32("testval")
	if err != nil {
		t.Fatalf("TestDocumentGetSetInt failed to get a uint32: %s", err.Error())
	}
	if testu32 != 80000 {
		t.Fatalf("TestDocumentGetSetInt uint32 value failure: wanted 80000, got %v", testu32)
	}

	if err := wm.AttachInt64("testval", 0x10000); err != nil {
		t.Fatalf("TestDocumentGetSetInt failed to set an int64: %s", err.Error())
	}
	tests64, err := wm.GetInt64("testval")
	if err != nil {
		t.Fatalf("TestDocumentGetSetInt failed to get an int64: %s", err.Error())
	}
	if tests64 != 0x10000 {
		t.Fatalf("TestDocumentGetSetInt int64 value failure: wanted 0x10000, got %x", tests64)
	}

	if err := wm.AttachUInt64("testval", 0x20000); err != nil {
		t.Fatalf("TestDocumentGetSetInt failed to set a uint64: %s", err.Error())
	}
	testu64, err := wm.GetUInt64("testval")
	if err != nil {
		t.Fatalf("TestDocumentGetSetInt failed to get a uint64: %s", err.Error())
	}
	if testu64 != 0x20000 {
		t.Fatalf("TestDocumentGetSetInt uint64 value failure: wanted 0x20000, got %x", testu64)
	}
}

func TestDocumentGetSetOther(t *testing.T) {

	wm := NewDocument("TestCommand")

	if err := wm.AttachString("testval", "Some test data"); err != nil {
		t.Fatalf("TestDocumentGetSetOther failed to set a string: %s", err.Error())
	}
	teststr, err := wm.GetString("testval")
	if err != nil {
		t.Fatalf("TestDocumentGetSetOther failed to get a string: %s", err.Error())
	}
	if teststr != "Some test data" {
		t.Fatalf("TestDocumentGetSetOther string value failure: wanted 'Some test data', got '%s'",
			teststr)
	}

	if err := wm.AttachBinary("testval", []byte("Some binary test data")); err != nil {
		t.Fatalf("TestDocumentGetSetOther failed to set a string: %s", err.Error())
	}
	testbin, err := wm.GetBinary("testval")
	if err != nil {
		t.Fatalf("TestDocumentGetSetOther failed to get a string: %s", err.Error())
	}
	if string(testbin) != "Some binary test data" {
		t.Fatalf("TestDocumentGetSetOther string value failure: "+
			"wanted 'Some binary test data', got '%s'", string(testbin))
	}

}
