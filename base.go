package oganesson

import (
	"errors"
	"time"
)

// This file contains code for wire-level 'packet' handling -- transmission of a byte slice over
// the network without any concern for size. Individual packets can be up to 64k, but the
// originating byte slices can be as big as can fit into memory. The mechanism makes it possible to
// send and receive arbitrary chunks of data over the network without worrying about the size of
// the network buffer.

// Segment objects are used for the structure of each packet, but the type codes are different.
// Packet type codes are just concerned with signifying being self-contained or multipart.

// Error Codes

var ErrNoInit = errors.New("not initialized")
var ErrEmptyData = errors.New("empty data")
var ErrNetworkError = errors.New("network error")
var ErrInvalidFrame = errors.New("invalid data frame")
var ErrInvalidMultipartFrame = errors.New("invalid multipart data frame")
var ErrServerError = errors.New("server error")
var ErrClientError = errors.New("client error")
var ErrTypeError = errors.New("type error")
var ErrKeyError = errors.New("key error")
var ErrNotFound = errors.New("not found")
var ErrSessionSetup = errors.New("incorrect session setup")
var ErrSessionMismatch = errors.New("session type mismatch")
var ErrTimedOut = errors.New("connection timeout")
var ErrInvalidContainer = errors.New("invalid segment container")
var ErrInvalidMsg = errors.New("invalid message")
var ErrSize = errors.New("invalid size")
var ErrEncryptionRequired = errors.New("encryption required")
var ErrMultipartSession = errors.New("multipart session error")
var ErrInvalidMultipartMsg = errors.New("invalid multipart message")
var ErrUnsupportedAlgorithm = errors.New("unsupported algorithm")
var ErrHashMismatch = errors.New("hash mismatch")

// Constants and Configurable Globals

// MaxCommandLength is the maximum number of bytes a command is permitted to be. Note that
// bulk transfers are not subject to this restriction -- just the initial command.
const MinCommandLength = 35

var MaxCommandLength = 16384
var DefaultBufferSize = uint16(65535)
var PacketSessionTimeout = 30 * time.Second
