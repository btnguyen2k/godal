package godal

// SortingField specifies sorting on a specific field.
//
// Available since v0.5.0
type SortingField struct {
	FieldName  string
	Descending bool
}

// ToSortingOpt is a helper function to convert this SortingField to SortingOpt.
func (sf *SortingField) ToSortingOpt() *SortingOpt {
	return (&SortingOpt{}).Add(sf)
}

// SortingOpt captures the ordering spec when fetching rows from storage.
//
// Available since v0.5.0
type SortingOpt struct {
	Fields []*SortingField
}

// Add appends fields to the sorting list.
func (so *SortingOpt) Add(fields ...*SortingField) *SortingOpt {
	if len(fields) > 0 {
		for _, field := range fields {
			if field != nil {
				so.Fields = append(so.Fields, field)
			}
		}
	}
	return so
}

/*----------------------------------------------------------------------*/

// FilterOperator represents an operator used in filter.
//
// Available since v0.5.0
type FilterOperator int

const (
	// FilterOpEqual is "equal" operator
	FilterOpEqual FilterOperator = iota
	// FilterOpNotEqual is "not equal" operator
	FilterOpNotEqual

	// FilterOpGreater is "greater" operator
	FilterOpGreater
	// FilterOpGreaterOrEqual is "greater or equal operator
	FilterOpGreaterOrEqual

	// FilterOpLess is "less than" operator
	FilterOpLess
	// FilterOpLessOrEqual is "less than or equal operator
	FilterOpLessOrEqual
)

// MakeFilter is helper function to build FilterOpt from an input map.
//   - input map is mapping of {field-name:field-value}.
//   - each filter element is built as <field-name> FilterOpEqual <value>.
//   - all filter elements are combined via FilterOptAnd.
//
// Available since v0.5.0
func MakeFilter(input map[string]interface{}) FilterOpt {
	result := &FilterOptAnd{}
	for k, v := range input {
		result.Add(&FilterOptFieldOpValue{FieldName: k, Operator: FilterOpEqual, Value: v})
	}
	if len(result.Filters) == 1 {
		return result.Filters[0]
	}
	return result
}

// FilterOpt is the abstract interface for specifying filter.
//
// Available since v0.5.0
type FilterOpt interface {
}

// FilterOptAnd combines two or more filters using AND clause.
//
// Available since v0.5.0
type FilterOptAnd struct {
	Filters []FilterOpt
}

// Add appends a filter to the list.
func (f *FilterOptAnd) Add(filter FilterOpt) *FilterOptAnd {
	if filter != nil {
		f.Filters = append(f.Filters, filter)
	}
	return f
}

// FilterOptOr combines two or more filters using OR clause.
//
// Available since v0.5.0
type FilterOptOr struct {
	Filters []FilterOpt
}

// Add appends a filter to the list.
func (f *FilterOptOr) Add(filter FilterOpt) *FilterOptOr {
	if filter != nil {
		f.Filters = append(f.Filters, filter)
	}
	return f
}

// FilterOptFieldOpValue represents single filter: <field> <operator> <value>.
type FilterOptFieldOpValue struct {
	FieldName string
	Operator  FilterOperator
	Value     interface{}
}

// FilterOptFieldOpField represents single filter: <field-left> <operator> <field-right>.
type FilterOptFieldOpField struct {
	FieldNameLeft  string
	Operator       FilterOperator
	FieldNameRight string
}

// FilterOptFieldIsNull represents single filter: <field> IS NULL.
type FilterOptFieldIsNull struct {
	FieldName string
}

// FilterOptFieldIsNotNull represents single filter: <field> IS NOT NULL.
type FilterOptFieldIsNotNull struct {
	FieldName string
}
