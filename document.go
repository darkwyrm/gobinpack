package oganesson

import (
	"io"

	"github.com/darkwyrm/oganesson/membufio"
)

// This section handles the WireMsg API, which uses JBitPack serialization to communicate at the
// session level. This is normally used for setting up encryption, but can als be used by
// applications wanting greater control over the implementation.

// WireMsg is a JBitPack document containing a string command name and optional associated data.
type WireMsg struct {
	MsgCode     string
	Attachments SegmentMap
}

// NewWireMsg creates a new message with the specified command name
func NewWireMsg(command string) *WireMsg {
	return &WireMsg{command, make(SegmentMap)}
}

// AttachInt8 adds an attachment to the message of the specified type. If the attached data exists,
// the value is updated.
func (wm *WireMsg) AttachInt8(name string, value int8) error {

	var seg Segment
	err := seg.SetInt8(value)
	if err != nil {
		return err
	}
	wm.Attachments[name] = seg

	return nil
}

// AttachUInt8 adds an attachment to the message of the specified type. If the attached data exists,
// the value is updated.
func (wm *WireMsg) AttachUInt8(name string, value uint8) error {

	var seg Segment
	err := seg.SetUInt8(value)
	if err != nil {
		return err
	}
	wm.Attachments[name] = seg

	return nil
}

// AttachInt16 adds an attachment to the message of the specified type. If the attached data exists,
// the value is updated.
func (wm *WireMsg) AttachInt16(name string, value int16) error {

	var seg Segment
	err := seg.SetInt16(value)
	if err != nil {
		return err
	}
	wm.Attachments[name] = seg

	return nil
}

// AttachUInt16 adds an attachment to the message of the specified type. If the attached data exists,
// the value is updated.
func (wm *WireMsg) AttachUInt16(name string, value uint16) error {

	var seg Segment
	err := seg.SetUInt16(value)
	if err != nil {
		return err
	}
	wm.Attachments[name] = seg

	return nil
}

// AttachInt32 adds an attachment to the message of the specified type. If the attached data exists,
// the value is updated.
func (wm *WireMsg) AttachInt32(name string, value int32) error {

	var seg Segment
	err := seg.SetInt32(value)
	if err != nil {
		return err
	}
	wm.Attachments[name] = seg

	return nil
}

// AttachUInt32 adds an attachment to the message of the specified type. If the attached data exists,
// the value is updated.
func (wm *WireMsg) AttachUInt32(name string, value uint32) error {

	var seg Segment
	err := seg.SetUInt32(value)
	if err != nil {
		return err
	}
	wm.Attachments[name] = seg

	return nil
}

// AttachInt64 adds an attachment to the message of the specified type. If the attached data exists,
// the value is updated.
func (wm *WireMsg) AttachInt64(name string, value int64) error {

	var seg Segment
	err := seg.SetInt64(value)
	if err != nil {
		return err
	}
	wm.Attachments[name] = seg

	return nil
}

// AttachUInt64 adds an attachment to the message of the specified type. If the attached data exists,
// the value is updated.
func (wm *WireMsg) AttachUInt64(name string, value uint64) error {

	var seg Segment
	err := seg.SetUInt64(value)
	if err != nil {
		return err
	}
	wm.Attachments[name] = seg

	return nil
}

// AttachString adds an attachment to the message of the specified type. If the attached data
// exists, the value is updated.
func (wm *WireMsg) AttachString(name string, value string) error {

	var seg Segment
	err := seg.SetString(value)
	if err != nil {
		return err
	}
	wm.Attachments[name] = seg

	return nil
}

// AttachBinary adds an attachment to the message of the specified type. If the attached data
// exists, the value is updated.
func (wm *WireMsg) AttachBinary(name string, value []byte) error {

	var seg Segment
	err := seg.SetBinary(value)
	if err != nil {
		return err
	}
	wm.Attachments[name] = seg

	return nil
}

// GetInt8 retrieves an attachment of the specified type
func (wm WireMsg) GetInt8(name string) (int8, error) {

	seg, ok := wm.Attachments[name]
	if !ok {
		return 0, ErrNotFound
	}
	return seg.GetInt8()
}

// GetUInt8 retrieves an attachment of the specified type
func (wm WireMsg) GetUInt8(name string) (uint8, error) {

	seg, ok := wm.Attachments[name]
	if !ok {
		return 0, ErrNotFound
	}
	return seg.GetUInt8()
}

// GetInt16 retrieves an attachment of the specified type
func (wm WireMsg) GetInt16(name string) (int16, error) {

	seg, ok := wm.Attachments[name]
	if !ok {
		return 0, ErrNotFound
	}
	return seg.GetInt16()
}

// GetUInt16 retrieves an attachment of the specified type
func (wm WireMsg) GetUInt16(name string) (uint16, error) {

	seg, ok := wm.Attachments[name]
	if !ok {
		return 0, ErrNotFound
	}
	return seg.GetUInt16()
}

// GetInt32 retrieves an attachment of the specified type
func (wm WireMsg) GetInt32(name string) (int32, error) {

	seg, ok := wm.Attachments[name]
	if !ok {
		return 0, ErrNotFound
	}
	return seg.GetInt32()
}

// GetUInt32 retrieves an attachment of the specified type
func (wm WireMsg) GetUInt32(name string) (uint32, error) {

	seg, ok := wm.Attachments[name]
	if !ok {
		return 0, ErrNotFound
	}
	return seg.GetUInt32()
}

// GetInt64 retrieves an attachment of the specified type
func (wm WireMsg) GetInt64(name string) (int64, error) {

	seg, ok := wm.Attachments[name]
	if !ok {
		return 0, ErrNotFound
	}
	return seg.GetInt64()
}

// GetUInt64 retrieves an attachment of the specified type
func (wm WireMsg) GetUInt64(name string) (uint64, error) {

	seg, ok := wm.Attachments[name]
	if !ok {
		return 0, ErrNotFound
	}
	return seg.GetUInt64()
}

// GetString retrieves an attachment of the specified type
func (wm WireMsg) GetString(name string) (string, error) {

	if len(name) == 0 || len(wm.Attachments) == 0 {
		return "", ErrEmptyData
	}

	df, ok := wm.Attachments[name]
	if !ok {
		return "", ErrNotFound
	}

	out, err := df.GetString()
	if err != nil {
		return "", err
	}

	return out, nil
}

// GetBinary retrieves an attachment of the specified type
func (wm WireMsg) GetBinary(name string) ([]byte, error) {

	if len(name) == 0 || len(wm.Attachments) == 0 {
		return nil, ErrEmptyData
	}

	df, ok := wm.Attachments[name]
	if !ok {
		return nil, ErrNotFound
	}

	out, err := df.GetBinary()
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (wm WireMsg) Has(index string) bool {
	if len(wm.Attachments) == 0 || len(index) == 0 {
		return false
	}
	_, ok := wm.Attachments[index]
	return ok
}

// Flatten is a convenience method that turns a WireMsg into a byte slice
func (wm WireMsg) Flatten() ([]byte, error) {

	// We don't check to see if Size() is zero because WireMsg objects have a minimum size even
	// when empty.
	bs := membufio.Make(wm.GetSize())

	if err := WriteSegment(&bs, DFDocumentStart, []byte{1}); err != nil {
		return nil, err
	}

	if err := WriteSegment(&bs, DFStringType, []byte(wm.MsgCode)); err != nil {
		return nil, err
	}

	if err := wm.Attachments.Write(&bs); err != nil {
		return nil, err
	}

	var docEnd Segment
	if err := docEnd.SetDocEnd((uint64(len(wm.Attachments)) * 2) + 2); err != nil {
		return nil, err
	}
	if err := docEnd.Write(&bs); err != nil {
		return nil, err
	}
	return bs.Buffer, nil
}

// Read attempts to read in a WireMsg from the given Reader.
func (wm *WireMsg) Read(r io.Reader) error {

	var s Segment

	if err := s.Read(r); err != nil {
		return err
	}
	if s.GetType() != DFDocumentStart {
		return ErrInvalidMsg
	}

	if err := s.Read(r); err != nil {
		return err
	}
	if s.GetType() != DFStringType {
		return ErrTypeError
	}

	wm.MsgCode, _ = s.GetString()
	wm.Attachments = make(SegmentMap)
	if err := wm.Attachments.Read(r); err != nil {
		return err
	}

	if err := s.Read(r); err != nil {
		return err
	}
	if s.GetType() != DFDocumentEnd {
		return ErrInvalidMsg
	}

	segCount, err := s.GetDocEnd()
	if err != nil {
		return err
	}
	if segCount != (uint64(len(wm.Attachments))*2)+2 {
		return ErrSize
	}
	return nil
}

// GetSize returns the size of the message when flattened
func (wm WireMsg) GetSize() uint64 {

	// Start with the DocStart segment size
	out := uint64(2)

	// The message code segment size varies based on its length
	if len(wm.MsgCode) < 65535 {
		out += 3 + uint64(len(wm.MsgCode))
	} else {
		out += 9 + uint64(len(wm.MsgCode))
	}

	out += wm.Attachments.GetSize()

	// DocEnd segment size
	out += 9
	return out
}

// Unflatten is a convenience method that initializes a WireMsg from a byte slice
func (wm *WireMsg) Unflatten(data []byte) error {

	bs := membufio.New(data)
	return wm.Read(&bs)
}

// Write dumps the WireMsg to the given Writer interface object.
func (wm *WireMsg) Write(w io.Writer) error {

	if err := WriteSegment(w, DFDocumentStart, []byte{1}); err != nil {
		return err
	}

	if err := WriteSegment(w, DFStringType, []byte(wm.MsgCode)); err != nil {
		return err
	}

	if err := wm.Attachments.Write(w); err != nil {
		return err
	}

	var docEnd Segment
	if err := docEnd.SetDocEnd((uint64(len(wm.Attachments)) * 2) + 2); err != nil {
		return err
	}
	if err := docEnd.Write(w); err != nil {
		return err
	}

	return nil
}
