package oganesson

import (
	"io"

	"github.com/darkwyrm/oganesson/membufio"
)

// This section handles the Document API, which uses JBitPack serialization to communicate at the
// session level. This is normally used for setting up encryption, but can als be used by
// applications wanting greater control over the implementation.

// Document is a JBitPack document containing a string command name and optional associated data.
type Document struct {
	Segments []Segment
}

// NewDocument creates a new document with the specified command name
func NewDocument() *Document {
	return &Document{make([]Segment, 0)}
}

// AttachInt8 adds an attachment to the document of the specified type. If the attached data exists,
// the value is updated.
func (doc *Document) AttachInt8(name string, value int8) error {

	var seg Segment
	err := seg.SetInt8(value)
	if err != nil {
		return err
	}
	doc.Segments = append(doc.Segments, seg)

	return nil
}

// AttachUInt8 adds an attachment to the document of the specified type. If the attached data exists,
// the value is updated.
func (doc *Document) AttachUInt8(name string, value uint8) error {

	var seg Segment
	err := seg.SetUInt8(value)
	if err != nil {
		return err
	}
	doc.Segments = append(doc.Segments, seg)

	return nil
}

// AttachInt16 adds an attachment to the document of the specified type. If the attached data exists,
// the value is updated.
func (doc *Document) AttachInt16(name string, value int16) error {

	var seg Segment
	err := seg.SetInt16(value)
	if err != nil {
		return err
	}
	doc.Segments = append(doc.Segments, seg)

	return nil
}

// AttachUInt16 adds an attachment to the document of the specified type. If the attached data exists,
// the value is updated.
func (doc *Document) AttachUInt16(name string, value uint16) error {

	var seg Segment
	err := seg.SetUInt16(value)
	if err != nil {
		return err
	}
	doc.Segments = append(doc.Segments, seg)

	return nil
}

// AttachInt32 adds an attachment to the document of the specified type. If the attached data exists,
// the value is updated.
func (doc *Document) AttachInt32(name string, value int32) error {

	var seg Segment
	err := seg.SetInt32(value)
	if err != nil {
		return err
	}
	doc.Segments = append(doc.Segments, seg)

	return nil
}

// AttachUInt32 adds an attachment to the document of the specified type. If the attached data exists,
// the value is updated.
func (doc *Document) AttachUInt32(name string, value uint32) error {

	var seg Segment
	err := seg.SetUInt32(value)
	if err != nil {
		return err
	}
	doc.Segments = append(doc.Segments, seg)

	return nil
}

// AttachInt64 adds an attachment to the document of the specified type. If the attached data exists,
// the value is updated.
func (doc *Document) AttachInt64(name string, value int64) error {

	var seg Segment
	err := seg.SetInt64(value)
	if err != nil {
		return err
	}
	doc.Segments = append(doc.Segments, seg)

	return nil
}

// AttachUInt64 adds an attachment to the document of the specified type. If the attached data exists,
// the value is updated.
func (doc *Document) AttachUInt64(name string, value uint64) error {

	var seg Segment
	err := seg.SetUInt64(value)
	if err != nil {
		return err
	}
	doc.Segments = append(doc.Segments, seg)

	return nil
}

// AttachString adds an attachment to the document of the specified type. If the attached data
// exists, the value is updated.
func (doc *Document) AttachString(name string, value string) error {

	var seg Segment
	err := seg.SetString(value)
	if err != nil {
		return err
	}
	doc.Segments = append(doc.Segments, seg)

	return nil
}

// AttachBinary adds an attachment to the document of the specified type. If the attached data
// exists, the value is updated.
func (doc *Document) AttachBinary(name string, value []byte) error {

	var seg Segment
	err := seg.SetBinary(value)
	if err != nil {
		return err
	}
	doc.Segments = append(doc.Segments, seg)

	return nil
}

// Flatten is a convenience method that turns a Document into a byte slice
func (doc Document) Flatten() ([]byte, error) {

	// We don't check to see if Size() is zero because Document objects have a minimum size even
	// when empty.
	bs := membufio.Make(doc.GetSize())

	if err := WriteSegment(&bs, DFDocumentStart, []byte{1}); err != nil {
		return nil, err
	}

	for _, seg := range doc.Segments {
		if err := seg.Write(&bs); err != nil {
			return nil, err
		}
	}

	var docEnd Segment
	if err := docEnd.SetDocEnd(uint64(len(doc.Segments))); err != nil {
		return nil, err
	}
	if err := docEnd.Write(&bs); err != nil {
		return nil, err
	}
	return bs.Buffer, nil
}

// Read attempts to read in a Document from the given Reader.
func (doc *Document) Read(r io.Reader) error {

	var s Segment

	if err := s.Read(r); err != nil {
		return err
	}
	if s.GetType() != DFDocumentStart {
		return ErrInvalidMsg
	}

	doc.Segments = make([]Segment, 0)

	if err := s.Read(r); err != nil {
		return err
	}
	for s.GetType() != DFDocumentEnd {
		doc.Segments = append(doc.Segments, s)

		if err := s.Read(r); err != nil {
			return err
		}
	}

	segCount, err := s.GetDocEnd()
	if err != nil {
		return err
	}

	if segCount != uint64(len(doc.Segments)) {
		return ErrSize
	}
	return nil
}

// GetSize returns the size of the document when flattened
func (doc Document) GetSize() uint64 {

	// Start with the DocStart segment size
	out := uint64(2)

	for _, s := range doc.Segments {
		out += s.GetSize()
	}

	// DocEnd segment size
	out += 9
	return out
}

// Unflatten is a convenience method that initializes a Document from a byte slice
func (doc *Document) Unflatten(data []byte) error {

	bs := membufio.New(data)
	return doc.Read(&bs)
}

// Write dumps the Document to the given Writer interface object.
func (doc *Document) Write(w io.Writer) error {

	if err := WriteSegment(w, DFDocumentStart, []byte{1}); err != nil {
		return err
	}

	for _, s := range doc.Segments {
		if err := s.Write(w); err != nil {
			return err
		}
	}

	var docEnd Segment
	if err := docEnd.SetDocEnd(uint64(len(doc.Segments))); err != nil {
		return err
	}
	if err := docEnd.Write(w); err != nil {
		return err
	}

	return nil
}
