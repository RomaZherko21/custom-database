package buffer_bool

import (
	"custom-database/internal/disk_manager"
	"time"
)

// PinCheckFunc функция для проверки, закреплена ли страница
type PinCheckFunc func(pageID disk_manager.PageID) bool

// LRUKCacheInterface не используется в коде, нужен для просмотра всех внешних методов LRUKCache
type LRUKCacheInterface interface {
	// Access обрабатывает обращение к странице.
	// Если страница еще не в кэше, то добавляем ее в LRU-K кэш.
	// Если уже в кэше, то обновляем ее (перемещаем в соответствующий список)
	Access(pageID disk_manager.PageID)
	// Evict вытесняет страницу из кэша, pageID получаем через GetVictim()
	// Возвращает true, если страница была вытеснена, false, если страница не была в кэше
	Evict(pageID disk_manager.PageID) bool
	// GetVictim возвращает страницу-кандидата для вытеснения с проверкой pin-статуса
	// Если isPinned == nil, то проверка pin-статуса не выполняется
	GetVictim(isPinned PinCheckFunc) disk_manager.PageID
}

// LRUKCache реализует LRU-K алгоритм для управления страницами
// K - количество последних обращений для принятия решения о вытеснении
// В ключах в Map мы храним структуры, так можно делать если в структуре легко сравниваемые поля (int, string, bool, float64)
// если бы внутри структуры был slice или map, то ключами они не могут быть
type LRUKCache struct {
	K             int                                    // Параметр K для LRU-K
	PageEntries   map[disk_manager.PageID]*PageEntry     // Карта страниц
	AccessHistory map[disk_manager.PageID]*AccessHistory // История обращений
	HotList       *LRUList                               // Список "горячих" страниц (K+ обращений)
	ColdList      *LRUList                               // Список "холодных" страниц (<K обращений)
	MaxSize       int                                    // Максимальный размер кэша
	CurrentSize   int                                    // Текущий размер кэша
}

// PageEntry представляет страницу в LRU-K кэше
type PageEntry struct {
	PageID      disk_manager.PageID
	AccessCount int       // Общее количество обращений за время пребывания в кэше, для определения в каком списке должна находиться страница
	LastAccess  time.Time // Время последнего обращения
	IsInHotList bool      // Находится ли в "горячем" списке
	Node        *Node     // Узел в соответствующем списке
}

// AccessHistory хранит историю последних K обращений к странице
type AccessHistory struct {
	PageID   disk_manager.PageID
	Accesses []time.Time // Времена последних K обращений
	Count    int         // Текущее количество записей, количество записей в массиве Accesses
}

// LRUList представляет двусвязный список для LRU
type LRUList struct {
	Head *Node
	Tail *Node
	Size int
}

// Node представляет узел в двусвязном списке
type Node struct {
	PageID       disk_manager.PageID
	LastUsedTime time.Time
	Prev         *Node
	Next         *Node
}

// NewLRUKCache создает новый LRU-K кэш
func NewLRUKCache(k int, maxSize int) *LRUKCache {
	return &LRUKCache{
		K:             k,
		PageEntries:   make(map[disk_manager.PageID]*PageEntry),
		AccessHistory: make(map[disk_manager.PageID]*AccessHistory),
		HotList:       &LRUList{},
		ColdList:      &LRUList{},
		MaxSize:       maxSize,
		CurrentSize:   0,
	}
}

// Access обрабатывает обращение к странице
func (cache *LRUKCache) Access(pageID disk_manager.PageID) {
	now := time.Now()

	// Обновляем историю обращений
	cache.updateAccessHistory(pageID, now)

	// Получаем или создаем запись о странице
	entry, exists := cache.PageEntries[pageID]
	if !exists {
		entry = &PageEntry{
			PageID: pageID,
			// Тут 0, т.к далее сделаем ++
			AccessCount: 0,
			LastAccess:  now,
			IsInHotList: false,
		}
		cache.PageEntries[pageID] = entry
		cache.CurrentSize++
	}

	// Обновляем счетчик обращений
	entry.AccessCount++
	entry.LastAccess = now

	// Определяем, в каком списке должна находиться страница
	shouldBeInHotList := entry.AccessCount >= cache.K

	// Если страница должна быть в "горячем" списке, но находится в "холодном"
	if shouldBeInHotList && !entry.IsInHotList {
		cache.moveToHotList(entry)
		return
	}

	// Если страница уже находится в списке, то просто обновляем позицию
	if entry.Node != nil {
		// Просто обновляем позицию в текущем списке
		cache.updatePosition(entry)
		return
	}

	// Добавляем страницу в соответствующий список, если она не находится в списке
	// (entry.Node == nil означает, что страница еще не добавлена ни в один список)
	if shouldBeInHotList {
		cache.addToHotList(entry)
	} else {
		cache.addToColdList(entry)
	}
}

// Evict вытесняет страницу из кэша
func (cache *LRUKCache) Evict(pageID disk_manager.PageID) bool {
	entry, exists := cache.PageEntries[pageID]
	if !exists {
		return false
	}

	// Удаляем из соответствующего списка
	if entry.IsInHotList {
		cache.removeFromHotList(entry)
	} else {
		cache.removeFromColdList(entry)
	}

	// Удаляем из карт
	delete(cache.PageEntries, pageID)
	delete(cache.AccessHistory, pageID)
	cache.CurrentSize--

	return true
}

// GetVictim возвращает страницу-кандидата для вытеснения с проверкой pin-статуса
// Если isPinned == nil, то проверка pin-статуса не выполняется
func (cache *LRUKCache) GetVictim(checkPinStatus PinCheckFunc) disk_manager.PageID {
	// Если проверка pin-статуса не нужна, используем простую логику
	if checkPinStatus == nil {
		// Сначала пытаемся найти страницу в "холодном" списке
		if cache.ColdList.Size > 0 {
			return cache.ColdList.Tail.PageID
		}

		// Если "холодный" список пуст, берем из "горячего"
		if cache.HotList.Size > 0 {
			return cache.HotList.Tail.PageID
		}

		// Если оба списка пусты, возвращаем пустой PageID
		return disk_manager.PageID{}
	}

	// Итеративно ищем незакрепленную страницу, начиная с наименее недавно использованных
	// Сначала ищем в "холодном" списке (от хвоста к голове)
	if cache.ColdList.Size > 0 {
		current := cache.ColdList.Tail
		for current != nil {
			if !checkPinStatus(current.PageID) {
				return current.PageID
			}
			current = current.Next
		}
	}

	// Если в "холодном" списке все страницы закреплены, ищем в "горячем"
	if cache.HotList.Size > 0 {
		current := cache.HotList.Tail
		for current != nil {
			if !checkPinStatus(current.PageID) {
				return current.PageID
			}
			current = current.Next
		}
	}

	// Если все страницы закреплены, возвращаем пустой PageID
	return disk_manager.PageID{}
}

// updateAccessHistory обновляет историю обращений к странице
func (cache *LRUKCache) updateAccessHistory(pageID disk_manager.PageID, accessTime time.Time) {
	history, exists := cache.AccessHistory[pageID]
	if !exists {
		history = &AccessHistory{
			PageID:   pageID,
			Accesses: make([]time.Time, 0, cache.K),
			// Тут 0, т.к далее сделаем ++
			Count: 0,
		}
		cache.AccessHistory[pageID] = history
	}

	// Добавляем новое обращение
	history.Accesses = append(history.Accesses, accessTime)
	history.Count++

	// Если превысили K, удаляем самое старое
	if history.Count > cache.K {
		history.Accesses = history.Accesses[1:]
		history.Count = cache.K
	}
}

// moveToHotList перемещает страницу в "горячий" список
func (cache *LRUKCache) moveToHotList(entry *PageEntry) {
	if entry.Node != nil {
		cache.removeFromColdList(entry)
	}
	cache.addToHotList(entry)
	entry.IsInHotList = true
}

// addToHotList добавляет страницу в "горячий" список
func (cache *LRUKCache) addToHotList(entry *PageEntry) {
	node := &Node{
		PageID:       entry.PageID,
		LastUsedTime: entry.LastAccess,
		Next:         nil,                // Новая нода будет Head (самая правая), Next = nil
		Prev:         cache.HotList.Head, // Новая нода указывает на старую голову как Prev
	}

	// Если список не пустой, обновляем связи старой головы
	if cache.HotList.Head != nil {
		cache.HotList.Head.Next = node // Старая голова теперь знает кто после неё
	} else {
		cache.HotList.Tail = node // Если список пуст, новая нода также хвост
	}

	// Новая нода становится головой
	cache.HotList.Head = node
	cache.HotList.Size++
	entry.Node = node
}

// addToColdList добавляет страницу в "холодный" список
func (cache *LRUKCache) addToColdList(entry *PageEntry) {
	node := &Node{
		PageID:       entry.PageID,
		LastUsedTime: entry.LastAccess,
		Next:         nil,                 // Новая нода будет Head (самая правая), Next = nil
		Prev:         cache.ColdList.Head, // Новая нода указывает на старую голову как Prev
	}

	// Если список не пустой, обновляем связи старой головы
	if cache.ColdList.Head != nil {
		cache.ColdList.Head.Next = node // Старая голова теперь знает кто после неё
	} else {
		cache.ColdList.Tail = node // Если список пуст, новая нода также хвост
	}

	// Новая нода становится головой
	cache.ColdList.Head = node
	cache.ColdList.Size++
	entry.Node = node
}

// removeFromHotList удаляет страницу из "горячего" списка
func (cache *LRUKCache) removeFromHotList(entry *PageEntry) {
	if entry.Node == nil {
		return
	}

	node := entry.Node

	if node.Prev != nil {
		node.Prev.Next = node.Next
	} else {
		cache.HotList.Tail = node.Next
	}

	if node.Next != nil {
		node.Next.Prev = node.Prev
	} else {
		cache.HotList.Head = node.Prev
	}

	cache.HotList.Size--
	entry.Node = nil
}

// removeFromColdList удаляет страницу из "холодного" списка
func (cache *LRUKCache) removeFromColdList(entry *PageEntry) {
	if entry.Node == nil {
		return
	}

	node := entry.Node

	if node.Prev != nil {
		node.Prev.Next = node.Next
	} else {
		cache.ColdList.Tail = node.Next
	}

	if node.Next != nil {
		node.Next.Prev = node.Prev
	} else {
		cache.ColdList.Head = node.Prev
	}

	cache.ColdList.Size--
	entry.Node = nil
}

// updatePosition обновляет позицию страницы в списке (перемещает в начало)
func (cache *LRUKCache) updatePosition(entry *PageEntry) {
	if entry.Node == nil {
		return
	}

	// Удаляем из текущей позиции
	if entry.IsInHotList {
		cache.removeFromHotList(entry)
		cache.addToHotList(entry)
	} else {
		cache.removeFromColdList(entry)
		cache.addToColdList(entry)
	}
}
