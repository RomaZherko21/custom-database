package disk_manager

import (
	"encoding/binary"
	"fmt"
)

type PageHeader struct {
	PageID      uint32
	RecordCount uint32 // Количество записей - для быстрого подсчета
	Lower       uint32 // Указывает на конец области слотов (начало свободного места)
	Upper       uint32 // Указывает на начало области данных, первый байт самой левой записи (конец свободного места)
	// FreeSpace = Upper - Lower
}

// Размер заголовка page файла
const PAGE_HEADER_SIZE = 16

func newPageHeader(pageID uint32) *PageHeader {
	return &PageHeader{
		PageID:      pageID,
		RecordCount: 0,
		Lower:       PAGE_HEADER_SIZE,
		Upper:       PAGE_SIZE,
	}
}

// Serialize сериализует PageHeader в байты
func (header *PageHeader) Serialize() []byte {
	data := make([]byte, PAGE_HEADER_SIZE)

	// Записываем PageID (байты 0-4)
	binary.BigEndian.PutUint32(data[0:4], header.PageID)

	// Записываем RecordCount (байты 4-8)
	binary.BigEndian.PutUint32(data[4:8], header.RecordCount)

	// Записываем Lower (байты 8-12)
	binary.BigEndian.PutUint32(data[8:12], header.Lower)

	// Записываем Upper (байты 12-16)
	binary.BigEndian.PutUint32(data[12:16], header.Upper)

	return data
}

func (header *PageHeader) Deserialize(data []byte) (*PageHeader, error) {
	if len(data) < PAGE_HEADER_SIZE {
		return nil, fmt.Errorf("insufficient data for page header")
	}

	return &PageHeader{
		PageID:      binary.BigEndian.Uint32(data[0:4]),
		RecordCount: binary.BigEndian.Uint32(data[4:8]),
		Lower:       binary.BigEndian.Uint32(data[8:12]),
		Upper:       binary.BigEndian.Uint32(data[12:16]),
	}, nil
}

type PageSlot struct {
	Offset uint32 // Смещение записи в странице
	Length uint32 // Длина записи
	Flags  uint32 // Флаги записи (0=active, 1=deleted)
}

const SLOT_SIZE = 12 // Размер слота (offset + длинна записи)

// Serialize сериализует PageSlot в байты
func (slot *PageSlot) Serialize() []byte {
	data := make([]byte, SLOT_SIZE)

	// Записываем Offset (байты 0-4)
	binary.BigEndian.PutUint32(data[0:4], slot.Offset)

	// Записываем Length (байты 4-8)
	binary.BigEndian.PutUint32(data[4:8], slot.Length)

	// Записываем Flags (байты 8-12)
	binary.BigEndian.PutUint32(data[8:12], slot.Flags)

	return data
}

func (slot *PageSlot) Deserialize(data []byte) (*PageSlot, error) {
	if len(data) < SLOT_SIZE {
		return nil, fmt.Errorf("insufficient data for page slot")
	}

	return &PageSlot{
		Offset: binary.BigEndian.Uint32(data[0:4]),
		Length: binary.BigEndian.Uint32(data[4:8]),
		Flags:  binary.BigEndian.Uint32(data[8:12]),
	}, nil
}

const TUPLE_LENGTH_FIELD_SIZE = 4
const TUPLE_NULL_BITMAP_SIZE = 4

// RawTuple сырые данные (Data []byte) одной записи (tuple) в таблице
type RawTuple struct {
	Length         uint32 // Длина записи
	NullBitmapSize uint32 // Размер null bitmap, нужен что бы мы понимали сколько байт занято для null bitmap и сделали правильный offset
	NullBitmap     []byte // Массив битов, представляющих null значения
	Data           []byte // Массив данных
}

// Serialize сериализует RawTuple в байты
func (tuple *RawTuple) Serialize() []byte {
	data := make([]byte, tuple.Length)

	// Записываем Length (байты 0-4)
	binary.BigEndian.PutUint32(data[0:4], tuple.Length)

	// Записываем NullBitmapSize (байты 4-8)
	binary.BigEndian.PutUint32(data[4:8], tuple.NullBitmapSize)

	// Записываем NullBitmap (байты 8-12)
	copy(data[8:8+tuple.NullBitmapSize], tuple.NullBitmap)

	// Записываем Data (байты 12-Length)
	copy(data[8+tuple.NullBitmapSize:tuple.Length], tuple.Data)

	return data
}

func (tuple *RawTuple) Deserialize(data []byte) (*RawTuple, error) {
	// Проверяем, что данные достаточны для заголовка
	if len(data) < 8 {
		return nil, fmt.Errorf("insufficient data for data tuple")
	}

	length := binary.BigEndian.Uint32(data[0:4])
	nullBitmapSize := binary.BigEndian.Uint32(data[4:8])

	if len(data) < int(length) {
		return nil, fmt.Errorf("insufficient data for data tuple")
	}

	return &RawTuple{
		Length:         length,
		NullBitmapSize: nullBitmapSize,
		NullBitmap:     data[8 : 8+nullBitmapSize],
		Data:           data[8+nullBitmapSize : length],
	}, nil
}

type RawPage struct {
	Header    PageHeader
	Slots     []PageSlot
	RawTuples []RawTuple
}

func newPage(pageID PageID) *RawPage {
	return &RawPage{
		Header:    *newPageHeader(pageID.PageNumber),
		Slots:     make([]PageSlot, 0),
		RawTuples: make([]RawTuple, 0),
	}
}

// Serialize сериализует Page в байты
func (page *RawPage) Serialize() []byte {
	data := make([]byte, PAGE_SIZE)

	// Записываем Header (байты 0-16)
	copy(data[0:PAGE_HEADER_SIZE], page.Header.Serialize())

	// Записываем Slots (байты 16-16+len(pages.Slots)*SLOT_SIZE)
	for i, slot := range page.Slots {
		slotOffset := PAGE_HEADER_SIZE + i*SLOT_SIZE
		copy(data[slotOffset:slotOffset+SLOT_SIZE], slot.Serialize())
	}

	// Записываем Rows с конца страницы
	for i, tuple := range page.RawTuples {
		startOffset := page.Slots[i].Offset
		endOffset := page.Slots[i].Offset + page.Slots[i].Length
		copy(data[startOffset:endOffset], tuple.Serialize())
	}

	return data
}

func (page *RawPage) Deserialize(data []byte) (*RawPage, error) {
	if len(data) < PAGE_SIZE {
		return nil, fmt.Errorf("insufficient data for page")
	}

	header, err := page.Header.Deserialize(data[0:PAGE_HEADER_SIZE])
	if err != nil {
		return nil, err
	}

	slots := make([]PageSlot, 0)
	for i := 0; i < int(header.RecordCount); i++ {
		slotOffset := PAGE_HEADER_SIZE + i*SLOT_SIZE

		slot, err := (&PageSlot{}).Deserialize(data[slotOffset : slotOffset+SLOT_SIZE])
		if err != nil {
			return nil, err
		}
		slots = append(slots, *slot)
	}

	tuples := make([]RawTuple, 0)
	for i := 0; i < int(header.RecordCount); i++ {
		startOffset := slots[i].Offset
		endOffset := slots[i].Offset + slots[i].Length

		tuple, err := (&RawTuple{}).Deserialize(data[startOffset:endOffset])
		if err != nil {
			return nil, err
		}
		tuples = append(tuples, *tuple)
	}

	return &RawPage{
		Header:    *header,
		Slots:     slots,
		RawTuples: tuples,
	}, nil
}
