package disk_manager

const (
	PAGE_SIZE        = 4096 // 4KB страница
	PAGE_HEADER_SIZE = 24   // Размер заголовка страницы
	SLOT_SIZE        = 4    // Размер слота (offset + длинна записи)
	MAX_SLOTS        = 1024 // Максимальное количество слотов
)
