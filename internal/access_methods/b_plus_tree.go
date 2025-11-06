package access_methods

import (
	"fmt"
	"sort"

	"custom-database/internal/disk_manager"
)

const (
	// MaxOrder - максимальное количество ключей в узле (для простоты используем 4)
	MaxOrder = 4
	// MinOrder - минимальное количество ключей в узле (для не root узлов)
	MinOrder = MaxOrder / 2
)

// IndexEntry представляет запись в индексе
type IndexEntry struct {
	Key     uint32               // Значение ID (f.e user_id)
	TupleID disk_manager.TupleID // PageID и SlotNumber
}

// BPlusTree представляет B+ дерево индекс
type BPlusTree struct {
	Root *Node
}

// Node представляет узел B+ дерева
type Node struct {
	IsLeaf   bool         // Является ли узел листовым
	Keys     []uint32     // Ключи (для внутренних узлов - разделители, для листовых - все ключи)
	Values   []IndexEntry // Значения (только для листовых узлов)
	Children []*Node      // Дочерние узлы (только для внутренних узлов)
	Parent   *Node        // Родительский узел
	Next     *Node        // Следующий листовой узел (для doubly linked list)
	Prev     *Node        // Предыдущий листовой узел (для doubly linked list)
}

// NewBPlusTree создает новое B+ дерево
func NewBPlusTree() *BPlusTree {
	root := &Node{
		IsLeaf: true,
		Keys:   make([]uint32, 0),
		Values: make([]IndexEntry, 0),
	}
	return &BPlusTree{Root: root}
}

// Insert вставляет новую запись в индекс
func (t *BPlusTree) Insert(key uint32, rowID disk_manager.TupleID) {
	entry := IndexEntry{Key: key, TupleID: rowID}

	// Если дерево пустое
	if t.Root == nil {
		t.Root = &Node{
			IsLeaf: true,
			Keys:   []uint32{key},
			Values: []IndexEntry{entry},
		}
		return
	}

	// Находим листовой узел для вставки
	leaf := t.findLeaf(key)

	// Вставляем в листовой узел
	t.insertIntoLeaf(leaf, entry)

	// Если узел переполнен, выполняем разделение
	if len(leaf.Keys) > MaxOrder {
		t.splitLeaf(leaf)
	}
}

// findLeaf находит листовой узел для данного ключа
func (t *BPlusTree) findLeaf(key uint32) *Node {
	node := t.Root
	for !node.IsLeaf {
		idx := linearSearch(len(node.Keys), func(i int) bool {
			return node.Keys[i] > key // [[] 10,[] 20,[] 30 []] search 15 -> 1
		})

		node = node.Children[idx]
	}
	return node
}

// insertIntoLeaf вставляет запись в листовой узел
func (t *BPlusTree) insertIntoLeaf(leaf *Node, entry IndexEntry) {
	// Находим позицию для вставки
	idx := linearSearch(len(leaf.Keys), func(i int) bool {
		return leaf.Keys[i] >= entry.Key
	})

	// Проверяем, не существует ли уже такой ключ
	if idx < len(leaf.Keys) && leaf.Keys[idx] == entry.Key {
		// Обновляем существующую запись
		leaf.Values[idx] = entry
		return
	}

	// Вставляем новый ключ и значение
	leaf.Keys = append(leaf.Keys, entry.Key)
	sort.Slice(leaf.Keys, func(i, j int) bool {
		return leaf.Keys[i] < leaf.Keys[j]
	})

	leaf.Values = append(leaf.Values, entry)
	sort.Slice(leaf.Values, func(i, j int) bool {
		return leaf.Values[i].Key < leaf.Values[j].Key
	})
}

// splitLeaf разделяет переполненный листовой узел
func (t *BPlusTree) splitLeaf(leaf *Node) {
	mid := len(leaf.Keys) / 2 // 5/2 == 2

	// Создаем новый правый узел
	rightLeaf := &Node{
		IsLeaf: true,
		Keys:   make([]uint32, len(leaf.Keys[mid:])),
		Values: make([]IndexEntry, len(leaf.Values[mid:])),
	}
	copy(rightLeaf.Keys, leaf.Keys[mid:])
	copy(rightLeaf.Values, leaf.Values[mid:])

	// Обрезаем левый узел
	leaf.Keys = leaf.Keys[:mid]
	leaf.Values = leaf.Values[:mid]

	// Обновляем связи doubly linked list
	rightLeaf.Next = leaf.Next
	rightLeaf.Prev = leaf
	if leaf.Next != nil {
		leaf.Next.Prev = rightLeaf
	}
	leaf.Next = rightLeaf

	// Вставляем разделитель в родительский узел
	t.insertIntoParent(leaf, rightLeaf.Keys[0], rightLeaf)
}

// insertIntoParent вставляет разделитель в родительский узел
func (t *BPlusTree) insertIntoParent(leftNode *Node, key uint32, rightNode *Node) {
	parent := leftNode.Parent

	// Если нет родителя, создаем новый корневой узел
	if parent == nil {
		t.Root = &Node{
			IsLeaf:   false,
			Keys:     []uint32{key},
			Children: []*Node{leftNode, rightNode},
		}
		leftNode.Parent = t.Root
		rightNode.Parent = t.Root
		return
	}

	// Вставляем ключ и дочерний узел
	parent.Keys = append(parent.Keys, key)
	sort.Slice(parent.Keys, func(i, j int) bool {
		return parent.Keys[i] < parent.Keys[j]
	})

	parent.Children = append(parent.Children, rightNode)
	sort.Slice(parent.Children, func(i, j int) bool {
		return parent.Children[i].Keys[0] < parent.Children[j].Keys[0]
	})

	rightNode.Parent = parent

	// Если родительский узел переполнен, разделяем его
	if len(parent.Keys) > MaxOrder {
		t.splitInnerNode(parent)
	}
}

// splitInnerNode разделяет переполненный внутренний узел
func (t *BPlusTree) splitInnerNode(innerNode *Node) {
	mid := len(innerNode.Keys) / 2
	separatorKey := innerNode.Keys[mid]

	// Создаем новый правый узел
	rightNode := &Node{
		IsLeaf:   false,
		Keys:     make([]uint32, len(innerNode.Keys[mid+1:])),
		Children: make([]*Node, len(innerNode.Children[mid+1:])),
	}
	copy(rightNode.Keys, innerNode.Keys[mid+1:])
	copy(rightNode.Children, innerNode.Children[mid+1:])

	// Обновляем родителя для всех дочерних узлов
	for _, child := range rightNode.Children {
		child.Parent = rightNode
	}

	// Обрезаем левый узел
	innerNode.Keys = innerNode.Keys[:mid]
	innerNode.Children = innerNode.Children[:mid+1]

	// Вставляем разделитель в родительский узел
	t.insertIntoParent(innerNode, separatorKey, rightNode)
}

// Delete удаляет запись из индекса
func (t *BPlusTree) Delete(key uint32) error {
	if t.Root == nil {
		return fmt.Errorf("tree is empty")
	}

	// Находим листовой узел с ключом
	leaf := t.findLeaf(key)

	// Ищем ключ в листовом узле
	idx := linearSearch(len(leaf.Keys), func(i int) bool {
		return leaf.Keys[i] == key
	})

	if idx >= len(leaf.Keys) {
		return fmt.Errorf("key %d not found", key)
	}

	// Удаляем ключ и значение
	leaf.Keys = append(leaf.Keys[:idx], leaf.Keys[idx+1:]...)
	leaf.Values = append(leaf.Values[:idx], leaf.Values[idx+1:]...)

	// Если узел стал < MinOrder и это не корень, выполняем слияние или заимствование
	if len(leaf.Keys) < MinOrder && leaf != t.Root {
		t.rebalanceLeaf(leaf)
	}

	// Если корень стал пустым (после удаления), удаляем его
	// TODO: обработка случая, когда корень стал пустым внутренним узлом
	if len(t.Root.Keys) == 0 && !t.Root.IsLeaf {
		if len(t.Root.Children) > 0 {
			t.Root = t.Root.Children[0]
			t.Root.Parent = nil
		}
	}

	return nil
}

// rebalanceLeaf выполняет ребалансировку листового узла
func (t *BPlusTree) rebalanceLeaf(leaf *Node) {
	parent := leaf.Parent
	if parent == nil {
		return
	}

	// Находим индекс текущего узла в родителе
	idx := linearSearch(len(parent.Children), func(i int) bool {
		return parent.Children[i] == leaf
	})
	if idx >= len(parent.Children) {
		return
	}

	// Пытаемся заимствовать у левого брата
	if idx > 0 { // [[]10,[] 20,[] 30 []]
		leftSibling := parent.Children[idx-1]
		if len(leftSibling.Keys) > MinOrder {
			// Берем последний элемент у левого брата
			lastKey := leftSibling.Keys[len(leftSibling.Keys)-1]
			lastValue := leftSibling.Values[len(leftSibling.Values)-1]

			leftSibling.Keys = leftSibling.Keys[:len(leftSibling.Keys)-1]
			leftSibling.Values = leftSibling.Values[:len(leftSibling.Values)-1]

			// Вставляем в начало текущего узла
			leaf.Keys = append([]uint32{lastKey}, leaf.Keys...)
			leaf.Values = append([]IndexEntry{lastValue}, leaf.Values...)

			// Обновляем разделитель в родителе
			parent.Keys[idx-1] = lastKey
			return
		}
	}

	// Пытаемся заимствовать у правого брата
	if idx < len(parent.Children)-1 { // [[]10,[] 20,[] 30 []]
		rightSibling := parent.Children[idx+1]
		if len(rightSibling.Keys) > MinOrder {
			// Берем первый элемент у правого брата
			firstKey := rightSibling.Keys[0]
			firstValue := rightSibling.Values[0]

			rightSibling.Keys = rightSibling.Keys[1:]
			rightSibling.Values = rightSibling.Values[1:]

			// Вставляем в конец текущего узла
			leaf.Keys = append(leaf.Keys, firstKey)
			leaf.Values = append(leaf.Values, firstValue)

			// Обновляем разделитель в родителе
			parent.Keys[idx] = rightSibling.Keys[0]
			return
		}
	}

	// Если не удалось заимствовать, выполняем слияние
	if idx > 0 {
		// Сливаем с левым братом
		leftSibling := parent.Children[idx-1]
		leftSibling.Keys = append(leftSibling.Keys, leaf.Keys...)
		leftSibling.Values = append(leftSibling.Values, leaf.Values...)

		// Обновляем связи doubly linked list
		if leaf.Next != nil {
			leaf.Next.Prev = leftSibling
		}
		leftSibling.Next = leaf.Next

		// Удаляем разделитель из родителя
		parent.Keys = append(parent.Keys[:idx-1], parent.Keys[idx:]...)
		parent.Children = append(parent.Children[:idx], parent.Children[idx+1:]...)

		// Если родитель стал < MinOrder, ребалансируем его
		if len(parent.Keys) < MinOrder && parent != t.Root {
			t.rebalanceInternal(parent)
		}
	} else if idx < len(parent.Children)-1 {
		// Сливаем с правым братом
		rightSibling := parent.Children[idx+1]
		leaf.Keys = append(leaf.Keys, rightSibling.Keys...)
		leaf.Values = append(leaf.Values, rightSibling.Values...)

		// Обновляем связи doubly linked list
		if rightSibling.Next != nil {
			rightSibling.Next.Prev = leaf
		}
		leaf.Next = rightSibling.Next

		// Удаляем разделитель из родителя
		parent.Keys = append(parent.Keys[:idx], parent.Keys[idx+1:]...)
		parent.Children = append(parent.Children[:idx+1], parent.Children[idx+2:]...)

		// Если родитель стал < MinOrder, ребалансируем его
		if len(parent.Keys) < MinOrder && parent != t.Root {
			t.rebalanceInternal(parent)
		}
	}
}

// rebalanceInternal выполняет ребалансировку внутреннего узла
func (t *BPlusTree) rebalanceInternal(node *Node) {
	parent := node.Parent
	if parent == nil {
		return
	}

	// Находим индекс текущего узла в родителе
	idx := linearSearch(len(parent.Children), func(i int) bool {
		return parent.Children[i] == node
	})
	if idx >= len(parent.Children) {
		return
	}

	// Пытаемся заимствовать у левого брата
	if idx > 0 {
		leftSibling := parent.Children[idx-1]
		if len(leftSibling.Keys) > MinOrder {
			// Берем последний ключ и дочерний узел у левого брата
			separatorKey := parent.Keys[idx-1]
			lastKey := leftSibling.Keys[len(leftSibling.Keys)-1]
			lastChild := leftSibling.Children[len(leftSibling.Children)-1]

			leftSibling.Keys = leftSibling.Keys[:len(leftSibling.Keys)-1]
			leftSibling.Children = leftSibling.Children[:len(leftSibling.Children)-1]

			// Вставляем в начало текущего узла
			node.Keys = append([]uint32{separatorKey}, node.Keys...)
			node.Children = append([]*Node{lastChild}, node.Children...)
			lastChild.Parent = node

			// Обновляем разделитель в родителе
			parent.Keys[idx-1] = lastKey
			return
		}
	}

	// Пытаемся заимствовать у правого брата
	if idx < len(parent.Children)-1 {
		rightSibling := parent.Children[idx+1]
		if len(rightSibling.Keys) > MinOrder {
			// Берем первый ключ и дочерний узел у правого брата
			separatorKey := parent.Keys[idx]
			firstKey := rightSibling.Keys[0]
			firstChild := rightSibling.Children[0]

			rightSibling.Keys = rightSibling.Keys[1:]
			rightSibling.Children = rightSibling.Children[1:]

			// Вставляем в конец текущего узла
			node.Keys = append(node.Keys, separatorKey)
			node.Children = append(node.Children, firstChild)
			firstChild.Parent = node

			// Обновляем разделитель в родителе
			parent.Keys[idx] = firstKey
			return
		}
	}

	// Если не удалось заимствовать, выполняем слияние
	if idx > 0 {
		// Сливаем с левым братом
		leftSibling := parent.Children[idx-1]
		separatorKey := parent.Keys[idx-1]

		leftSibling.Keys = append(leftSibling.Keys, separatorKey)
		leftSibling.Keys = append(leftSibling.Keys, node.Keys...)
		leftSibling.Children = append(leftSibling.Children, node.Children...)

		// Обновляем родителя для всех дочерних узлов
		for _, child := range node.Children {
			child.Parent = leftSibling
		}

		// Удаляем разделитель из родителя
		parent.Keys = append(parent.Keys[:idx-1], parent.Keys[idx:]...)
		parent.Children = append(parent.Children[:idx], parent.Children[idx+1:]...)

		// Если родитель стал < MinOrder, ребалансируем его
		if len(parent.Keys) < MinOrder && parent != t.Root {
			t.rebalanceInternal(parent)
		}
	} else if idx < len(parent.Children)-1 {
		// Сливаем с правым братом
		rightSibling := parent.Children[idx+1]
		separatorKey := parent.Keys[idx]

		node.Keys = append(node.Keys, separatorKey)
		node.Keys = append(node.Keys, rightSibling.Keys...)
		node.Children = append(node.Children, rightSibling.Children...)

		// Обновляем родителя для всех дочерних узлов
		for _, child := range rightSibling.Children {
			child.Parent = node
		}

		// Удаляем разделитель из родителя
		parent.Keys = append(parent.Keys[:idx], parent.Keys[idx+1:]...)
		parent.Children = append(parent.Children[:idx+1], parent.Children[idx+2:]...)

		// Если родитель стал < MinOrder, ребалансируем его
		if len(parent.Keys) < MinOrder && parent != t.Root {
			t.rebalanceInternal(parent)
		}
	}
}

// GetRange выполняет запрос диапазона значений
// Возвращает все записи с ключами в диапазоне [startKey, endKey]
func (t *BPlusTree) GetRange(startKey, endKey uint32) []*IndexEntry {
	if t.Root == nil {
		return []*IndexEntry{}
	}

	var results []*IndexEntry

	// Находим начальный листовой узел
	startLeaf := t.findLeaf(startKey)

	// Проходим по листовым узлам, используя doubly linked list
	current := startLeaf
	for current != nil {
		for i, key := range current.Keys {
			if key >= startKey && key <= endKey {
				results = append(results, &current.Values[i])
			} else if key > endKey {
				// Если ключ превышает endKey, прекращаем поиск
				return results
			}
		}

		// Переходим к следующему листовому узлу
		current = current.Next

		// Если первый ключ следующего узла превышает endKey, прекращаем
		if current != nil && len(current.Keys) > 0 && current.Keys[0] > endKey {
			break
		}
	}

	return results
}

// Get возвращает TupleID для заданного ключа
func (t *BPlusTree) Get(key uint32) (*IndexEntry, bool) {
	if t.Root == nil {
		return nil, false
	}

	leaf := t.findLeaf(key)

	idx := linearSearch(len(leaf.Keys), func(i int) bool {
		return leaf.Keys[i] == key
	})

	if idx < len(leaf.Keys) && leaf.Keys[idx] == key {
		return &IndexEntry{Key: key, TupleID: leaf.Values[idx].TupleID}, true
	}

	return nil, false
}

// linearSearch выполняет линейный поиск в слайсе ключей
// Возвращает наименьший индекс i в [0, n), при котором f(i) истинно
// Если такого индекса нет, возвращает n
// Используется линейный поиск, так как количество ключей в узле небольшое (MaxOrder = 4)
func linearSearch(n int, f func(int) bool) int {
	for i := 0; i < n; i++ {
		if f(i) {
			return i
		}
	}
	return n
}
