package oganesson

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/darkwyrm/oganesson/membufio"
)

var ErrInvalidSegment = errors.New("invalid field")
var ErrInvalidKey = errors.New("invalid key")
var ErrSegmentSize = errors.New("invalid field size")
var ErrIO = errors.New("i/o error")

const (
	DFUnknownType = iota

	// Collections of Segments are called documents. The DFDocumentStart type code has no
	// associated data. The DFDocumentEnd field has a 16-bit length followed by a string containing
	// the number of items in the document.
	DFDocumentStart
	DFDocumentEnd

	// Fixed-length fields that only have their type code and the data stored in MSB order
	DFInt8Type
	DFUInt8Type
	DFInt16Type
	DFUInt16Type
	DFInt32Type
	DFUInt32Type
	DFInt64Type
	DFUInt64Type
	DFBoolType
	DFFloat32Type
	DFFloat64Type

	// Variable-length fields that have a maximum size of 64K, which should be large enough for
	// most purposes. The type code is followed by a uint16 which denotes the data size. Strings
	// are required to be UTF-8 compliant.
	DFStringType
	DFBinaryType

	// For when you need to transport data by the bargeload. The type code is followed by a uint64
	// which denotes the data size and then the data follows after that.
	DFHugeStringType
	DFHugeBinaryType

	// Maps and lists are just a series of Segments. The Segment with the map or list type code
	// behaves like a uint16 field in that a 16-bit unsigned integer follows the type code and
	// indicates the number of items to follow that belong to that container. For maps, the item
	// count indicates the number of key-value pairs and a list indicates the actual number of
	// fields. For complexity reasons maps and lists may not be nested. The LargeMap and LargeList
	// types use an unsigned 32-bit integer to indicate the number of items.
	DFMapType
	DFListType
	DFLargeMapType
	DFLargeListType

	// This code isn't used for anything except for type code validity checking. It MUST be last
	// in this list!
	DFUpperBound
)

func isTypeCodeValid(typecode uint8) bool {
	return typecode < DFUpperBound && typecode > 0
}

// sizeSegmentSize returns the number of bytes used by a type's size field, such as 8 for
// DFHugeStringType
func sizeSegmentSize(typeCode uint8) uint8 {

	switch typeCode {
	case DFStringType, DFBinaryType:
		return 2
	case DFHugeStringType, DFHugeBinaryType:
		return 8
	}
	return 0
}

// fixedSegmentSize returns the size, in bytes, of a fixed-size segment or 0 on error
func fixedSegmentSize(typeCode uint8) uint8 {
	switch typeCode {
	case DFInt8Type, DFUInt8Type, DFBoolType, DFDocumentStart:
		return 1
	case DFInt16Type, DFUInt16Type, DFMapType, DFListType:
		return 2
	case DFInt32Type, DFUInt32Type, DFFloat32Type, DFLargeMapType, DFLargeListType:
		return 4
	case DFInt64Type, DFUInt64Type, DFFloat64Type, DFDocumentEnd:
		return 8
	}
	return 0
}

// The Segment structure is the foundation of the JBitPack data serialization format
type Segment struct {
	Type  uint8
	Value []byte
}

type SegmentMap map[string]Segment
type SegmentList []Segment

// SizeOf returns the does the same thing as encoding/binary.Size(). It shouldn't exist, but it
// does. Why? Because despite how common uint, int, and string types are, binary.Size() returns -1
// for all of them. Infuriating.
func SizeOf(v interface{}) uint64 {

	if s, ok := v.(string); ok {
		return uint64(len(s))
	}
	if s, ok := v.(*string); ok {
		return uint64(len(*s))
	}
	switch v.(type) {
	case uint, int:
		return strconv.IntSize / 8
	default:
		return uint64(binary.Size(v))
	}
}

// GetType returns the data type of the segment
func (seg *Segment) GetType() uint8 {
	return seg.Type
}

// GetSize returns the size of the field in bytes when serialized
func (seg *Segment) GetSize() uint64 {
	intSize := fixedSegmentSize(seg.Type)
	if intSize != 0 {
		return uint64(intSize) + 1
	}

	sizeSize := uint64(sizeSegmentSize(seg.Type))
	return uint64(len(seg.Value)) + sizeSize + 1
}

// Read attempts to set the value of the object from the I/O reader given to it
func (seg *Segment) Read(r io.Reader) error {

	typeBuffer := make([]byte, 1)
	bytesRead, err := r.Read(typeBuffer)
	if err != nil {
		return err
	}
	if bytesRead != 1 {
		return ErrIO
	}

	if !isTypeCodeValid(typeBuffer[0]) {
		return ErrInvalidSegment
	}

	var sizeWriter []byte
	var payloadSize uint64
	sizeSize := sizeSegmentSize(typeBuffer[0])
	if sizeSize != 0 {
		sizeWriter = make([]byte, sizeSize)

		bytesRead, err = r.Read(sizeWriter)
		if err != nil {
			return err
		}

		if bytesRead != int(sizeSize) {
			return ErrIO
		}

		// The size bytes are in network order (MSB), so this makes dealing with CPU architecture much
		// less of a headache regardless of what archictecture this is compiled for.
		payloadSize = (uint64(sizeWriter[0]) << 8) + uint64(sizeWriter[1])
	} else {
		payloadSize = uint64(fixedSegmentSize(typeBuffer[0]))
	}

	seg.Type = typeBuffer[0]

	payloadBuffer := make([]byte, payloadSize)
	bytesRead, err = r.Read(payloadBuffer)
	if err != nil {
		return err
	}

	if bytesRead != int(payloadSize) {
		return ErrSegmentSize
	}
	seg.Value = payloadBuffer[:bytesRead]

	return nil
}

// Write dumps the flattened version of the field to the writer. It is just a wrapper around
// WriteSegment()
func (seg Segment) Write(w io.Writer) error {
	return WriteSegment(w, seg.Type, seg.Value)
}

// GetDocStart retrieves the version value from a DocumentStart segment or returns an error
func (seg Segment) GetDocStart() (uint8, error) {
	if seg.Type != DFDocumentStart {
		return 0, ErrTypeError
	}
	if len(seg.Value) != 1 {
		return 0, ErrSize
	}
	return seg.Value[0], nil
}

// GetDocEnd retrieves the version value from a DocumentEnd segment or returns an error
func (seg Segment) GetDocEnd() (uint64, error) {
	if seg.Type != DFDocumentEnd {
		return 0, ErrTypeError
	}
	bs := membufio.New(seg.Value)
	var data uint64
	if err := binary.Read(&bs, binary.BigEndian, &data); err != nil {
		return 0, err
	}
	return data, nil
}

// GetInt8 retrieves the value from an Int8 segment or returns an error
func (seg Segment) GetInt8() (int8, error) {
	if seg.Type != DFInt8Type {
		return 0, ErrTypeError
	}
	if len(seg.Value) != 1 {
		return 0, ErrSize
	}
	return int8(seg.Value[0]), nil
}

// GetUInt8 retrieves the value from a UInt8 segment or returns an error
func (seg Segment) GetUInt8() (uint8, error) {
	if seg.Type != DFUInt8Type {
		return 0, ErrTypeError
	}
	if len(seg.Value) != 1 {
		return 0, ErrSize
	}
	return seg.Value[0], nil
}

// GetInt16 retrieves the value from an Int16 segment or returns an error
func (seg Segment) GetInt16() (int16, error) {
	if seg.Type != DFInt16Type {
		return 0, ErrTypeError
	}
	bs := membufio.New(seg.Value)
	var data int16
	if err := binary.Read(&bs, binary.BigEndian, &data); err != nil {
		return 0, err
	}
	return data, nil
}

// GetUInt16 retrieves the value from a UInt16 segment or returns an error
func (seg Segment) GetUInt16() (uint16, error) {

	if seg.Type != DFUInt16Type {
		return 0, ErrTypeError
	}
	bs := membufio.New(seg.Value)
	var data uint16
	if err := binary.Read(&bs, binary.BigEndian, &data); err != nil {
		return 0, err
	}
	return data, nil
}

// GetInt32 retrieves the value from an Int32 segment or returns an error
func (seg Segment) GetInt32() (int32, error) {
	if seg.Type != DFInt32Type {
		return 0, ErrTypeError
	}
	bs := membufio.New(seg.Value)
	var data int32
	if err := binary.Read(&bs, binary.BigEndian, &data); err != nil {
		return 0, err
	}
	return data, nil
}

// GetUInt32 retrieves the value from a UInt32 segment or returns an error
func (seg Segment) GetUInt32() (uint32, error) {
	if seg.Type != DFUInt32Type {
		return 0, ErrTypeError
	}
	bs := membufio.New(seg.Value)
	var data uint32
	if err := binary.Read(&bs, binary.BigEndian, &data); err != nil {
		return 0, err
	}
	return data, nil
}

// GetInt64 retrieves the value from an Int64 segment or returns an error
func (seg Segment) GetInt64() (int64, error) {
	if seg.Type != DFInt64Type {
		return 0, ErrTypeError
	}
	bs := membufio.New(seg.Value)
	var data int64
	if err := binary.Read(&bs, binary.BigEndian, &data); err != nil {
		return 0, err
	}
	return data, nil
}

// GetUInt64 retrieves the value from a UInt64 segment or returns an error
func (seg Segment) GetUInt64() (uint64, error) {

	if seg.Type != DFUInt64Type {
		return 0, ErrTypeError
	}
	bs := membufio.New(seg.Value)
	var data uint64
	if err := binary.Read(&bs, binary.BigEndian, &data); err != nil {
		return 0, err
	}
	return data, nil
}

// GetBool retrieves the value from a Bool segment or returns an error
func (seg Segment) GetBool() (bool, error) {
	if seg.Type != DFBoolType {
		return false, ErrTypeError
	}
	if len(seg.Value) != 1 {
		return false, ErrSize
	}
	return seg.Value[0] != 0, nil
}

// GetFloat32 retrieves the value from a Float32 segment or returns an error
func (seg Segment) GetFloat32() (float32, error) {
	if seg.Type != DFFloat32Type {
		return 0, ErrTypeError
	}
	bs := membufio.New(seg.Value)
	var data float32
	if err := binary.Read(&bs, binary.BigEndian, &data); err != nil {
		return 0, err
	}
	return data, nil
}

// GetFloat64 retrieves the value from a Float64 segment or returns an error
func (seg Segment) GetFloat64() (float64, error) {
	if seg.Type != DFFloat64Type {
		return 0, ErrTypeError
	}
	bs := membufio.New(seg.Value)
	var data float64
	if err := binary.Read(&bs, binary.BigEndian, &data); err != nil {
		return 0, err
	}
	return data, nil
}

// GetString retrieves the value from a String segment or returns an error
func (seg Segment) GetString() (string, error) {
	if seg.Type != DFStringType && seg.Type != DFHugeStringType {
		return "", ErrTypeError
	}
	return string(seg.Value), nil
}

// GetBinary retrieves the value from a Binary segment or returns an error
func (seg Segment) GetBinary() ([]byte, error) {
	if seg.Type != DFBinaryType && seg.Type != DFHugeBinaryType {
		return nil, ErrTypeError
	}
	return seg.Value, nil
}

// GetMapIndex retrieves size of a map from its index segment or returns an error
func (seg Segment) GetMapIndex() (uint64, error) {

	bs := membufio.New(seg.Value)

	switch seg.Type {
	case DFMapType:
		var out uint16
		if err := binary.Read(&bs, binary.BigEndian, &out); err != nil {
			return 0, err
		}
		return uint64(out), nil
	case DFLargeMapType:
		var out uint64
		if err := binary.Read(&bs, binary.BigEndian, &out); err != nil {
			return 0, err
		}
		return out, nil
	default:
		return 0, ErrTypeError
	}
}

// GetListIndex retrieves size of a list from its index segment or returns an error
func (seg Segment) GetListIndex() (uint64, error) {

	bs := membufio.New(seg.Value)

	switch seg.Type {
	case DFListType:
		var out uint16
		if err := binary.Read(&bs, binary.BigEndian, &out); err != nil {
			return 0, err
		}
		return uint64(out), nil
	case DFLargeListType:
		var out uint64
		if err := binary.Read(&bs, binary.BigEndian, &out); err != nil {
			return 0, err
		}
		return out, nil
	default:
		return 0, ErrTypeError
	}
}

// SetDocStart sets the Segment's value and type
func (seg *Segment) SetDocStart(version uint8) error {
	seg.Type = DFDocumentStart

	valueLen := SizeOf(version)
	segLen := uint64(len(seg.Value))

	if segLen != valueLen {
		seg.Value = make([]byte, valueLen)
	}

	seg.Value[0] = uint8(version)
	return nil
}

// SetDocEnd sets the Segment's value and type
func (seg *Segment) SetDocEnd(segcount uint64) error {
	seg.Type = DFDocumentEnd

	valueLen := SizeOf(segcount)
	segLen := uint64(len(seg.Value))

	if segLen != valueLen {
		seg.Value = make([]byte, valueLen)
	}

	bs := membufio.New(seg.Value)
	return binary.Write(&bs, binary.BigEndian, segcount)
}

// SetInt8 sets the Segment's value and type
func (seg *Segment) SetInt8(value int8) error {
	seg.Type = DFInt8Type

	valueLen := SizeOf(value)
	segLen := uint64(len(seg.Value))

	if segLen != valueLen {
		seg.Value = make([]byte, valueLen)
	}

	seg.Value[0] = uint8(value)
	return nil
}

// SetUInt8 sets the Segment's value and type
func (seg *Segment) SetUInt8(value uint8) error {
	seg.Type = DFUInt8Type

	valueLen := SizeOf(value)
	segLen := uint64(len(seg.Value))

	if segLen != valueLen {
		seg.Value = make([]byte, valueLen)
	}

	seg.Value[0] = value
	return nil
}

// SetInt16 sets the Segment's value and type
func (seg *Segment) SetInt16(value int16) error {
	seg.Type = DFInt16Type

	valueLen := SizeOf(value)
	segLen := uint64(len(seg.Value))

	if segLen != valueLen {
		seg.Value = make([]byte, valueLen)
	}

	bs := membufio.New(seg.Value)
	return binary.Write(&bs, binary.BigEndian, value)
}

// SetUInt16 sets the Segment's value and type
func (seg *Segment) SetUInt16(value uint16) error {
	seg.Type = DFUInt16Type

	valueLen := SizeOf(value)
	segLen := uint64(len(seg.Value))

	if segLen != valueLen {
		seg.Value = make([]byte, valueLen)
	}

	bs := membufio.New(seg.Value)
	return binary.Write(&bs, binary.BigEndian, value)
}

// SetInt32 sets the Segment's value and type
func (seg *Segment) SetInt32(value int32) error {
	seg.Type = DFInt32Type

	valueLen := SizeOf(value)
	segLen := uint64(len(seg.Value))

	if segLen != valueLen {
		seg.Value = make([]byte, valueLen)
	}

	bs := membufio.New(seg.Value)
	return binary.Write(&bs, binary.BigEndian, value)
}

// SetUInt32 sets the Segment's value and type
func (seg *Segment) SetUInt32(value uint32) error {
	seg.Type = DFUInt32Type

	valueLen := SizeOf(value)
	segLen := uint64(len(seg.Value))

	if segLen != valueLen {
		seg.Value = make([]byte, valueLen)
	}

	bs := membufio.New(seg.Value)
	return binary.Write(&bs, binary.BigEndian, value)
}

// SetInt64 sets the Segment's value and type
func (seg *Segment) SetInt64(value int64) error {
	seg.Type = DFInt64Type

	valueLen := SizeOf(value)
	segLen := uint64(len(seg.Value))

	if segLen != valueLen {
		seg.Value = make([]byte, valueLen)
	}

	bs := membufio.New(seg.Value)
	return binary.Write(&bs, binary.BigEndian, value)
}

// SetUInt64 sets the Segment's value and type
func (seg *Segment) SetUInt64(value uint64) error {
	seg.Type = DFUInt64Type

	valueLen := SizeOf(value)
	segLen := uint64(len(seg.Value))

	if segLen != valueLen {
		seg.Value = make([]byte, valueLen)
	}

	bs := membufio.New(seg.Value)
	return binary.Write(&bs, binary.BigEndian, value)
}

// SetBool sets the Segment's value and type
func (seg *Segment) SetBool(value bool) error {
	seg.Type = DFBoolType

	valueLen := SizeOf(value)
	segLen := uint64(len(seg.Value))

	if segLen != valueLen {
		seg.Value = make([]byte, valueLen)
	}

	if value {
		seg.Value[0] = 1
	} else {
		seg.Value[0] = 0
	}
	return nil
}

// SetFloat32 sets the Segment's value and type
func (seg *Segment) SetFloat32(value float32) error {
	seg.Type = DFFloat32Type

	valueLen := SizeOf(value)
	segLen := uint64(len(seg.Value))

	if segLen != valueLen {
		seg.Value = make([]byte, valueLen)
	}

	bs := membufio.New(seg.Value)
	return binary.Write(&bs, binary.BigEndian, value)
}

// SetFloat64 sets the Segment's value and type
func (seg *Segment) SetFloat64(value float64) error {
	seg.Type = DFFloat64Type

	valueLen := SizeOf(value)
	segLen := uint64(len(seg.Value))

	if segLen != valueLen {
		seg.Value = make([]byte, valueLen)
	}

	bs := membufio.New(seg.Value)
	return binary.Write(&bs, binary.BigEndian, value)
}

// SetString sets the Segment's value and type
func (seg *Segment) SetString(value string) error {
	if len(value) > 65535 {
		seg.Type = DFHugeStringType
	} else {
		seg.Type = DFStringType
	}

	valueLen := uint64(len(value))
	segLen := uint64(len(seg.Value))

	if segLen != valueLen {
		seg.Value = make([]byte, valueLen)
	}

	copy(seg.Value, value)
	return nil
}

// SetBinary sets the Segment's value and type
func (seg *Segment) SetBinary(value []byte) error {
	if len(value) > 65535 {
		seg.Type = DFHugeBinaryType
	} else {
		seg.Type = DFBinaryType
	}

	valueLen := uint64(len(value))
	segLen := uint64(len(seg.Value))

	if segLen != valueLen {
		seg.Value = make([]byte, valueLen)
	}

	copy(seg.Value, value)
	return nil
}

// SetMapIndex sets the Segment's value and type
func (seg *Segment) SetMapIndex(value SegmentMap) error {

	if len(value) > 65535 {
		seg.Type = DFLargeMapType
	} else {
		seg.Type = DFMapType
	}

	valueLen := uint64(fixedSegmentSize(seg.Type))
	segLen := uint64(len(seg.Value))
	if segLen != valueLen {
		seg.Value = make([]byte, valueLen)
	}

	bs := membufio.New(seg.Value)
	if seg.Type == DFLargeMapType {
		itemCount := uint64(len(value))
		return binary.Write(&bs, binary.BigEndian, itemCount)
	}
	itemCount := uint16(len(value))
	return binary.Write(&bs, binary.BigEndian, itemCount)
}

// SetListIndex sets the Segment's value and type
func (seg *Segment) SetListIndex(value SegmentList) error {

	if len(value) > 65535 {
		seg.Type = DFLargeListType
	} else {
		seg.Type = DFListType
	}

	valueLen := uint64(fixedSegmentSize(seg.Type))
	segLen := uint64(len(seg.Value))
	if segLen != valueLen {
		seg.Value = make([]byte, valueLen)
	}

	bs := membufio.New(seg.Value)
	if seg.Type == DFLargeListType {
		itemCount := uint64(len(value))
		return binary.Write(&bs, binary.BigEndian, itemCount)
	}
	itemCount := uint16(len(value))
	return binary.Write(&bs, binary.BigEndian, itemCount)
}

// ToString formats a Segment into a string
func (seg Segment) ToString() string {

	switch seg.Type {
	case DFUnknownType:
		return "Unknown"
	case DFDocumentStart:
		v, err := seg.GetDocStart()
		if err != nil {
			return "DocumentStart=" + err.Error()
		}
		return fmt.Sprintf("DocumentStart=%v", v)
	case DFDocumentEnd:
		v, err := seg.GetDocEnd()
		if err != nil {
			return "DocumentEnd=" + err.Error()
		}
		return fmt.Sprintf("DocumentEnd=%v", v)
	case DFInt8Type:
		v, err := seg.GetInt8()
		if err != nil {
			return "Int8=" + err.Error()
		}
		return fmt.Sprintf("Int8=%v", v)
	case DFUInt8Type:
		v, err := seg.GetUInt8()
		if err != nil {
			return "UInt8=" + err.Error()
		}
		return fmt.Sprintf("UInt8=%v", v)
	case DFInt16Type:
		v, err := seg.GetInt16()
		if err != nil {
			return "Int16=" + err.Error()
		}
		return fmt.Sprintf("Int16=%v", v)
	case DFUInt16Type:
		v, err := seg.GetUInt16()
		if err != nil {
			return "UInt16=" + err.Error()
		}
		return fmt.Sprintf("UInt16=%v", v)
	case DFInt32Type:
		v, err := seg.GetInt32()
		if err != nil {
			return "Int32=" + err.Error()
		}
		return fmt.Sprintf("Int32=%v", v)
	case DFUInt32Type:
		v, err := seg.GetUInt32()
		if err != nil {
			return "UInt32=" + err.Error()
		}
		return fmt.Sprintf("UInt32=%v", v)
	case DFInt64Type:
		v, err := seg.GetInt64()
		if err != nil {
			return "Int64=" + err.Error()
		}
		return fmt.Sprintf("Int64=%v", v)
	case DFUInt64Type:
		v, err := seg.GetUInt64()
		if err != nil {
			return "UInt64=" + err.Error()
		}
		return fmt.Sprintf("UInt64=%v", v)
	case DFBoolType:
		v, err := seg.GetBool()
		if err != nil {
			return "Bool=" + err.Error()
		}
		if v {
			return "Bool=True"
		} else {
			return "Bool=False"
		}
	case DFFloat32Type:
		v, err := seg.GetFloat32()
		if err != nil {
			return "Float32=" + err.Error()
		}
		return fmt.Sprintf("Float32=%v", v)
	case DFFloat64Type:
		v, err := seg.GetFloat64()
		if err != nil {
			return "Float64=" + err.Error()
		}
		return fmt.Sprintf("Float64=%v", v)
	case DFStringType, DFHugeStringType:
		if len(seg.Value) > 32 {
			return `String="` + string(seg.Value[:33]) + `"`
		} else {
			return `String="` + string(seg.Value) + `"`
		}
	case DFBinaryType, DFHugeBinaryType:
		if len(seg.Value) > 32 {
			return fmt.Sprintf("Binary=%v", seg.Value[:33])
		} else {
			return fmt.Sprintf("Binary=%v", seg.Value)
		}
	case DFMapType:
		v, err := seg.GetUInt16()
		if err != nil {
			return "Map=" + err.Error()
		}
		return fmt.Sprintf("Map=%v", v)
	case DFLargeMapType:
		v, err := seg.GetUInt64()
		if err != nil {
			return "LargeMap=" + err.Error()
		}
		return fmt.Sprintf("LargeMap=%v", v)
	case DFListType:
		v, err := seg.GetUInt16()
		if err != nil {
			return "List=" + err.Error()
		}
		return fmt.Sprintf("List=%v", v)
	case DFLargeListType:
		v, err := seg.GetUInt64()
		if err != nil {
			return "LargeList=" + err.Error()
		}
		return fmt.Sprintf("LargeList=%v", v)
	}
	return "InvalidType"
}

// UnflattenSegment tries to create a Segment from the data at the beginning of the byte slice
// given to it
func UnflattenSegment(p []byte) (Segment, error) {
	var out Segment
	bs := membufio.New(p)
	if err := out.Read(&bs); err != nil {
		return out, err
	}

	return out, nil
}

// FlattenSegment exists so that Segments can be turned into byte arrays without having to create
// a Segment instance
func FlattenSegment(fieldType uint8, fieldValue []byte) ([]byte, error) {

	valueLen := uint64(len(fieldValue))
	if valueLen == 0 {
		return nil, ErrEmptyData
	}

	sizeSize := sizeSegmentSize(fieldType)

	bufio := membufio.Make(valueLen + uint64(sizeSize) + 1)
	bufio.WriteByte(fieldType)
	switch sizeSize {
	case 2:
		binary.Write(&bufio, binary.BigEndian, uint16(valueLen))
	case 4:
		binary.Write(&bufio, binary.BigEndian, uint32(valueLen))
	case 8:
		binary.Write(&bufio, binary.BigEndian, uint64(valueLen))
	default:
		return nil, ErrSegmentSize
	}
	bufio.Write(fieldValue)

	return bufio.Buffer, nil
}

// WriteSegment exists so that Segments can be written to I/O without necessarily having to create
// a Segment instance
func WriteSegment(w io.Writer, fieldType uint8, fieldValue []byte) error {
	payloadSize := len(fieldValue)

	// Write the type code

	bytesWritten, err := w.Write([]byte{byte(fieldType)})
	if err != nil {
		return err
	}
	if bytesWritten != 1 {
		return ErrIO
	}

	// Write the size field

	sizeSize := sizeSegmentSize(fieldType)
	if sizeSize != 0 {
		sizeWriter := membufio.Make(uint64(sizeSize))
		switch sizeSize {
		case 2:
			binary.Write(&sizeWriter, binary.BigEndian, uint16(payloadSize))
		case 4:
			binary.Write(&sizeWriter, binary.BigEndian, uint32(payloadSize))
		case 8:
			binary.Write(&sizeWriter, binary.BigEndian, uint64(payloadSize))
		}
		bytesWritten, err = w.Write(sizeWriter.Buffer)
		if err != nil {
			return err
		}
		if bytesWritten != 2 {
			return ErrIO
		}
	}

	// Write the payload itself

	bytesWritten, err = w.Write(fieldValue)
	if err != nil {
		return err
	}
	if bytesWritten != payloadSize {
		return ErrNetworkError
	}

	return nil
}

// CountSegments returns the number of fields in the buffer. Nested Segment instances, such as
// items in a map, are not included in the count.
func CountSegments(p []byte) (int, error) {
	if len(p) < 4 {
		return 0, nil
	}
	var out int

	bs := membufio.New(p)
	typeBuffer := make([]byte, 1)
	for {
		// Read the type code for the current segment
		bytesRead, err := bs.Read(typeBuffer)
		if bytesRead != 1 || !isTypeCodeValid(typeBuffer[0]) {
			return 0, ErrInvalidSegment
		}
		if err != nil {
			return 0, err
		}

		// Determine the number of bytes to skip based on if the payload is fixed-size or variable
		var payloadSize uint64
		sizeSize := sizeSegmentSize(typeBuffer[0])
		if sizeSize == 0 {
			// Fixed-size data segment
			payloadSize = uint64(fixedSegmentSize(typeBuffer[0]))
			if payloadSize == 0 {
				return 0, ErrInvalidSegment
			}
		} else {
			switch sizeSize {
			case 2:
				var temp16 uint16
				err = binary.Read(&bs, binary.BigEndian, &temp16)
				if err != nil {
					return 0, ErrInvalidSegment
				}
				payloadSize = uint64(temp16)
			case 4:
				var temp32 uint32
				err = binary.Read(&bs, binary.BigEndian, &temp32)
				if err != nil {
					return 0, ErrInvalidSegment
				}
				payloadSize = uint64(temp32)
			case 8:
				err = binary.Read(&bs, binary.BigEndian, &payloadSize)
				if err != nil {
					return 0, ErrInvalidSegment
				}
			default:
				return 0, ErrInvalidSegment
			}
		}

		// Every field has to have a payload of at least 1 byte
		if payloadSize == 0 {
			return 0, ErrInvalidSegment
		}
		out++

		_, err = bs.Seek(int64(payloadSize), io.SeekCurrent)
		if err != nil {
			if err.Error() != "EOF" {
				return 0, err
			}
			break
		}
		if bs.IsEOF() {
			break
		}
	}

	return out, nil
}

// Clear empties the SegmentMap instance
func (sm SegmentMap) Clear() {
	for k := range sm {
		delete(sm, k)
	}
}

// Read attempts to read a string-Segment map from a byte buffer. Note that this call will overwrite
// existing keys with new data
func (sm SegmentMap) Read(r io.Reader) error {

	var countSegment Segment
	err := countSegment.Read(r)
	if err != nil {
		return err
	}

	pairCount, err := countSegment.GetMapIndex()
	if err != nil {
		return err
	}
	if pairCount == 0 {
		return nil
	}

	var keySegment Segment
	for i := uint64(0); i < pairCount; i++ {
		err = keySegment.Read(r)
		if err != nil {
			return err
		}
		if keySegment.Type != DFStringType {
			return ErrInvalidKey
		}

		var valueSegment Segment
		err = valueSegment.Read(r)
		if err != nil {
			return err
		}

		sm[string(keySegment.Value)] = valueSegment
	}

	return nil
}

// Write flattens a SegmentMap to an io.Writer.
func (sm SegmentMap) Write(w io.Writer) error {

	var countSegment Segment
	if uint64(len(sm)) > 65535 {
		countSegment.SetUInt64(uint64(len(sm)))
		countSegment.Type = DFLargeMapType
	} else {
		countSegment.SetUInt16(uint16(len(sm)))
		countSegment.Type = DFMapType
	}
	err := countSegment.Write(w)
	if err != nil {
		return err
	}

	if len(sm) == 0 {
		return nil
	}

	var keySegment Segment
	for k, v := range sm {
		keySegment.SetString(k)
		if err := keySegment.Write(w); err != nil {
			return err
		}

		if err := v.Write(w); err != nil {
			return err
		}
	}
	return nil
}

// GetSize returns the size of the buffer needed to contain all flattened elements
func (sm SegmentMap) GetSize() uint64 {
	var out uint64
	if len(sm) == 0 {
		return 3
	}

	for k, v := range sm {
		out += 3 + uint64(len(k)) + v.GetSize()
	}
	return out + 3
}

// Clear empties the SegmentList instance
func (sl *SegmentList) Clear() SegmentList {
	*sl = (*sl)[:0]
	return *sl
}

// GetSize returns the size of the buffer needed to contain all flattened elements
func (sl SegmentList) GetSize() uint64 {
	var out uint64
	if len(sl) == 0 {
		return 3
	}

	for _, item := range sl {
		out += item.GetSize()
	}
	return out + 3
}

// Read attempts to read a SegmentList from a byte buffer. Note that this call will append the
// new items to the list if it is not empty
func (sl *SegmentList) Read(p []byte) error {

	bs := membufio.New(p)

	var countSegment Segment
	err := countSegment.Read(&bs)
	if err != nil {
		return err
	}

	// Like SegmentMap::Read(), we're going to hack around using using Get() because of language
	// limitations. :(
	var itemCount uint64
	countReader := membufio.New(countSegment.Value)
	switch countSegment.Type {
	case DFListType:
		var tempInt uint16
		err = binary.Read(&countReader, binary.BigEndian, &tempInt)
		if err != nil {
			return err
		}
		itemCount = uint64(tempInt)
	case DFLargeListType:
		err = binary.Read(&countReader, binary.BigEndian, &itemCount)
		if err != nil {
			return err
		}
	}

	if itemCount == 0 {
		return nil
	}

	for i := uint64(0); i < itemCount; i++ {
		var itemSegment Segment
		err = itemSegment.Read(&bs)
		if err != nil {
			return err
		}

		*sl = append(*sl, itemSegment)
	}

	return nil
}

// Write flattens a SegmentList to an io.Writer.
func (sl SegmentList) Write(w io.Writer) error {

	var countSegment Segment
	if uint64(len(sl)) > 65535 {
		countSegment.SetUInt64(uint64(len(sl)))
		countSegment.Type = DFLargeListType
	} else {
		countSegment.SetUInt16(uint16(len(sl)))
		countSegment.Type = DFListType
	}
	err := countSegment.Write(w)
	if err != nil {
		return err
	}

	if len(sl) == 0 {
		return nil
	}

	for _, i := range sl {
		if err := i.Write(w); err != nil {
			return err
		}
	}
	return nil
}
