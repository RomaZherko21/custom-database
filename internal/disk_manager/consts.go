package disk_manager

// Размер страницы
// Помним что чтение и запись с диска (HDD, SSD) происходит блоками (по 4KB, но для разных устройств может быть разный размер блока),
// поэтому разумно выбрать размер страницы равный 4KB
const PAGE_SIZE = 4096

// Начальный ID страницы, последующие ID страниц увеличиваются на 1
const PAGE_INITIAL_ID = 1

// Пути к файлам
const META_FILE_PATH = "tables/%s.meta"
const PAGE_DIRECTORY_FILE_PATH = "tables/%s.dir"
const DATA_FILE_PATH = "tables/%s.data"
const TABLE_LIST_FILE_PATH = "tables/list/table_list.bin"

// Магические числа
// Используются в самом начале файла для проверки корректности формата файла
const META_FILE_MAGIC_NUMBER = 0x9ABCDEF0
const PAGE_DIRECTORY_MAGIC_NUMBER = 0x8ABCDEF1
const DATA_FILE_MAGIC_NUMBER = 0x12345678
const TABLES_LIST_MAGIC_NUMBER = 0x7ABCDEF2

// Ограничения
const TABLE_NAME_MAX_LENGTH = 32
const COLUMN_NAME_MAX_LENGTH = 32
const MAX_TABLE_COLUMNS_AMOUNT = 32 // Максимальное количество колонок в таблице
