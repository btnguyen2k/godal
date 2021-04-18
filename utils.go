package godal

// SortingField specifies sorting on a specific field.
//
// Available since v0.5.0
type SortingField struct {
	FieldName  string
	Descending bool
}

// SortingOpt captures the ordering spec when fetching rows from storage.
//
// Available since v0.5.0
type SortingOpt struct {
	Fields []*SortingField
}

// Add appends a field sorting spec to the sorting list.
func (so *SortingOpt) Add(field *SortingField) *SortingOpt {
	if field != nil {
		so.Fields = append(so.Fields, field)
	}
	return so
}
