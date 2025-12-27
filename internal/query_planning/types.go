package query_planning

// ColumnDefinition представляет определение колонки
type ColumnDefinition struct {
	Name     string
	DataType string // "INT", "TEXT"
}

// Value представляет значение (для INSERT и WHERE)
type Value struct {
	Type   ValueType
	IntVal int32
	StrVal string
	IsNull bool
}

// OrderByColumn представляет колонку для сортировки
type OrderByColumn struct {
	ColumnName string // Имя колонки
	Direction  string // "ASC" или "DESC"
}
