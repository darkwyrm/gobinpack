package oganesson

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func FrameSessionTestSetup(sync chan int, port string) {
	// Wait until the test is ready and then go from there
	<-sync
	time.Sleep(time.Millisecond * 100)

	senderconn, err := net.Dial("tcp", "127.0.0.1:"+port)
	if err != nil {
		panic(fmt.Sprintf("Error connecting to test server: %s", err.Error()))
	}

	s := NewPacketRequester(senderconn)
	s.Timeout = time.Minute * 5

	err = s.InitRequester()
	if err != nil {
		panic(fmt.Sprintf("Requester init failed: %s", err.Error()))
	}

	err = s.Write([]byte("ThisIsATestMessage"))
	if err != nil {
		panic(err)
	}
}

// TestFrameSession  and its corresponding setup function cover session setup and transmitting and
// receiving a single frame over the wire
func TestFrameSession(t *testing.T) {
	sync := make(chan int)
	go FrameSessionTestSetup(sync, "2999")

	listener, err := net.Listen("tcp", "127.0.0.1:2999")
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

	err = s.InitResponder()
	if err != nil {
		panic(fmt.Sprintf("Responder init failed: %s", err.Error()))
	}

	data, err := s.Read()
	if err != nil {
		t.Fatalf("Error receiving size test message: %s", err.Error())
	}

	if string(data) != "ThisIsATestMessage" {
		t.Fatalf("Data mismatch: %s", data)
	}
}

func WriteMultipartMessageSetup(sync chan int, port string) {
	// Wait until the test is ready and then go from there
	<-sync
	time.Sleep(time.Millisecond * 100)

	senderconn, err := net.Dial("tcp", "127.0.0.1:"+port)
	if err != nil {
		panic(fmt.Sprintf("Error connecting to test server: %s", err.Error()))
	}

	s := NewPacketRequester(senderconn)
	s.Timeout = time.Minute * 5

	err = s.InitRequester()
	if err != nil {
		panic(fmt.Sprintf("Requester init failed: %s", err.Error()))
	}

	message := make([]byte, 2601)
	for i, c := range []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ") {
		index := 100 * i
		copy(message[index:index+101], []byte(strings.Repeat(string(c), 100)))
	}
	err = s.Write(message)

	if err != nil {
		panic(err)
	}
}

// TestWriteMultipartMessage and its corresponding setup function test only the Packet type
// multipart sending code
func TestWriteMultipartMessage(t *testing.T) {
	sync := make(chan int)
	go WriteMultipartMessageSetup(sync, "3000")

	listener, err := net.Listen("tcp", "127.0.0.1:3000")
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

	s := NewPacketResponder(conn, 1024)
	s.Timeout = time.Minute * 5
	err = s.InitResponder()
	if err != nil {
		t.Fatalf("Responder init failure: %s", err.Error())
	}

	frame := NewDataFrame(1024)
	err = frame.Read(s.Connection)
	if err != nil {
		t.Fatalf("Error receiving size chunk message: %s", err.Error())
	}

	var totalSize uint64
	totalSize, err = strconv.ParseUint(string(frame.GetPayload()), 10, 64)
	if err != nil {
		t.Fatalf("Error parsing total size for multipart message: %s", err.Error())
	}

	fmt.Printf("Total size: %v", totalSize)
	msgParts := make([]string, 0)
	err = frame.Read(s.Connection)
	if err != nil {
		t.Fatalf("Error receiving first chunk message: %s", err.Error())
	}
	msgParts = append(msgParts, string(frame.GetPayload()))

	err = frame.Read(s.Connection)
	if err != nil {
		t.Fatalf("Error receiving second chunk message: %s", err.Error())
	}
	msgParts = append(msgParts, string(frame.GetPayload()))

	err = frame.Read(s.Connection)
	if err != nil {
		t.Fatalf("Error receiving third chunk message: %s", err.Error())
	}
	msgParts = append(msgParts, string(frame.GetPayload()))

	// All message chunks received. Now reassemble, check total size, and unmarshal

	msgData := strings.Join(msgParts, "")
	if uint64(len(msgData)) != totalSize {
		t.Fatalf("Total message size mismatch. Expected: %d. Received %d", totalSize, len(msgData))
	}

	message := make([]byte, 2601)
	for i, c := range []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ") {
		index := 100 * i
		copy(message[index:index+101], []byte(strings.Repeat(string(c), 100)))
	}
	if string(msgData) != string(message) {
		t.Fatalf("Message data mismatch")
	}
}

// TestReadMultipartMessage uses the same setup function as TestWriteMultipartMessage to test
// both multipart sending and receiving code in the Packet class
func TestReadMultipartMessage1(t *testing.T) {
	MaxCommandLength = 300
	sync := make(chan int)
	go WriteMultipartMessageSetup(sync, "3001")

	listener, err := net.Listen("tcp", "127.0.0.1:3001")
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
	err = s.InitResponder()
	if err != nil {
		t.Fatalf("Responder init failure: %s", err.Error())
	}

	_, err = s.Read()
	if err != nil {
		t.Fatalf("Failure to read WirePacket: %s", err.Error())
	}
}
