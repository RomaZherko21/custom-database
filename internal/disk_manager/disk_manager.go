package disk_manager

import (
	"fmt"
)

// DiskManager интерфейс для работы с диском
// Используется в Buffer Pool
type DiskManager interface {
	// Data Base
	CreateDataBase() error
	// Tables
	// CreateTable - создает таблицу, помним что пустой начальной страницы не будет,
	// ее нужно создать напрямую через в buffer pool через AddNewPage
	CreateTable(tableName string, columns []ColumnInfo) error
	DropTable(tableName string) error

	// Tables List - метод для чтения списка таблиц
	ReadTableList() (*TablesList, error)

	// MetaFile
	ReadMetaFile(tableName string) (*MetaData, error)
	WriteMetaFile(tableName string, metaFile *MetaData) (*MetaData, error)

	// PageDirectory
	ReadPageDirectory(tableName string) (*PageDirectory, error)
	WritePageDirectory(tableName string, pageDirectory *PageDirectory) (*PageDirectory, error)

	// DataHeaders
	ReadDataHeaders(tableName string) (*DataFileHeader, error)
	WriteDataHeaders(tableName string, dataHeaders *DataFileHeader) (*DataFileHeader, error)

	// Pages
	ReadPage(tableName string, pageID PageID) (*Page, error)
	WritePage(tableName string, pageID PageID, page *Page) (*Page, error)
	// AddNewPage - добавляет новую страницу в таблицу
	AddNewPage(tableName string, pageID PageID) (*Page, error)
}

type diskManager struct {
}

func NewDiskManager() DiskManager {
	return &diskManager{}
}

// ========================== DataBase ==========================

func (dm *diskManager) CreateDataBase() error {
	_, err := createTableListFile()
	if err != nil {
		return err
	}

	return nil
}

// ========================== CreateTable ==========================

func (dm *diskManager) CreateTable(tableName string, columns []ColumnInfo) error {
	_, err := createMetaFile(tableName, columns)
	if err != nil {
		return err
	}

	// Создаем page directory файл
	_, err = createPageDirectoryFile(tableName)
	if err != nil {
		return err
	}

	// Создаем data файл
	_, err = createDataFile(tableName)
	if err != nil {
		return err
	}

	// Добавляем страницу в PageDirectory
	_, err = readPageDirectory(tableName)
	if err != nil {
		return err
	}

	// Обновляем список таблиц
	_, err = addTableInList(tableName)
	if err != nil {
		return fmt.Errorf("failed to update tables list: %w", err)
	}

	return nil
}

func (dm *diskManager) DropTable(tableName string) error {
	err := deleteMetaFile(tableName)
	if err != nil {
		return err
	}

	err = deletePageDirectory(tableName)
	if err != nil {
		return err
	}

	err = deleteDataFile(tableName)
	if err != nil {
		return err
	}

	// Обновляем список таблиц
	_, err = deleteTableInList(tableName)
	if err != nil {
		return fmt.Errorf("failed to update tables list: %w", err)
	}

	return nil
}

// ========================== Table List ==========================

func (dm *diskManager) ReadTableList() (*TablesList, error) {
	return readTableListFile()
}

// ========================== MetaFile ==========================

func (dm *diskManager) ReadMetaFile(tableName string) (*MetaData, error) {
	return readMetaFile(tableName)
}

func (dm *diskManager) WriteMetaFile(tableName string, metaFile *MetaData) (*MetaData, error) {
	return writeMetaFile(tableName, metaFile)
}

// ========================== PageDirectory ==========================

func (dm *diskManager) ReadPageDirectory(tableName string) (*PageDirectory, error) {
	return readPageDirectory(tableName)
}

func (dm *diskManager) WritePageDirectory(tableName string, pageDirectory *PageDirectory) (*PageDirectory, error) {
	return writePageDirectory(tableName, pageDirectory)
}

// ========================== DataHeaders ==========================

func (dm *diskManager) ReadDataHeaders(tableName string) (*DataFileHeader, error) {
	return readDataFileHeaders(tableName)
}

func (dm *diskManager) WriteDataHeaders(tableName string, dataHeaders *DataFileHeader) (*DataFileHeader, error) {
	return writeDataFileHeaders(tableName, dataHeaders)
}

// ========================== Page ==========================

func (dm *diskManager) ReadPage(tableName string, pageID PageID) (*Page, error) {
	df, err := readDataFileHeader(tableName)
	if err != nil {
		return nil, err
	}

	page, err := df.readPage(tableName, pageID)
	if err != nil {
		return nil, err
	}

	metaData, err := readMetaFile(tableName)
	if err != nil {
		return nil, err
	}

	// переводим data=[]byte в осмысленные данные
	rows := make([]Row, 0)
	for _, row := range page.RawTuples {
		rowData, err := ConvertRawTupleToRow(row, metaData.Columns)
		if err != nil {
			return nil, err
		}
		rows = append(rows, rowData)
	}

	return &Page{
		Header:  page.Header,
		Slots:   page.Slots,
		Rows:    rows,
		Columns: metaData.Columns,
	}, nil
}

func (dm *diskManager) WritePage(tableName string, pageID PageID, page *Page) (*Page, error) {
	df, err := readDataFileHeader(tableName)
	if err != nil {
		return nil, err
	}

	err = df.writePage(tableName, pageID, ConvertPageToRawPage(page))
	if err != nil {
		return nil, err
	}

	// читаем страницу
	result, err := dm.ReadPage(tableName, pageID)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (dm *diskManager) AddNewPage(tableName string, pageID PageID) (*Page, error) {
	df, err := readDataFileHeader(tableName)
	if err != nil {
		return nil, err
	}

	err = df.addPage(tableName)
	if err != nil {
		return nil, err
	}

	return dm.ReadPage(tableName, pageID)
}
