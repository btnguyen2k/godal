package sql

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/btnguyen2k/prom/sql"
)

func TestNewPlaceholderGeneratorQuestion(t *testing.T) {
	testName := "TestNewPlaceholderGeneratorQuestion"
	f := NewPlaceholderGeneratorQuestion()

	numRoutines := 8
	numRunsPerRoutines := 100
	result := make([]string, numRoutines*numRunsPerRoutines)
	var wg sync.WaitGroup
	wg.Add(numRoutines)
	for i := 0; i < numRoutines; i++ {
		go func(wg *sync.WaitGroup, id, numRunsPerRoutines int, result []string, f PlaceholderGenerator) {
			defer wg.Done()
			for i := 0; i < numRunsPerRoutines; i++ {
				index := numRunsPerRoutines*id + i
				result[index] = f(strconv.Itoa(index))
			}
		}(&wg, i, numRunsPerRoutines, result, f)
	}
	wg.Wait()

	for i, placeholder := range result {
		expected := "?"
		if placeholder != expected {
			t.Fatalf("%s failed: expected %#v at index %d but received %#v", testName, expected, i, result)
		}
	}
}

func TestNewPlaceholderGeneratorDollarN(t *testing.T) {
	testName := "TestNewPlaceholderGeneratorDollarN"
	f := NewPlaceholderGeneratorDollarN()

	numRoutines := 8
	numRunsPerRoutines := 100
	result := make([]string, numRoutines*numRunsPerRoutines)
	var wg sync.WaitGroup
	wg.Add(numRoutines)
	for i := 0; i < numRoutines; i++ {
		go func(wg *sync.WaitGroup, id, numRunsPerRoutines int, result []string, f PlaceholderGenerator) {
			defer wg.Done()
			for i := 0; i < numRunsPerRoutines; i++ {
				index := numRunsPerRoutines*id + i
				result[index] = f(strconv.Itoa(index))
			}
		}(&wg, i, numRunsPerRoutines, result, f)
	}
	wg.Wait()

	sort.Slice(result, func(i, j int) bool {
		vi, _ := strconv.Atoi(result[i][1:])
		vj, _ := strconv.Atoi(result[j][1:])
		return vi < vj
	})
	for i, placeholder := range result {
		expected := "$" + strconv.Itoa(i+1)
		if placeholder != expected {
			t.Fatalf("%s failed: expected %#v at index %d but received %#v", testName, expected, i, result)
		}
	}
}

func TestNewPlaceholderGeneratorColonN(t *testing.T) {
	testName := "TestNewPlaceholderGeneratorColonN"
	f := NewPlaceholderGeneratorColonN()

	numRoutines := 8
	numRunsPerRoutines := 100
	result := make([]string, numRoutines*numRunsPerRoutines)
	var wg sync.WaitGroup
	wg.Add(numRoutines)
	for i := 0; i < numRoutines; i++ {
		go func(wg *sync.WaitGroup, id, numRunsPerRoutines int, result []string, f PlaceholderGenerator) {
			defer wg.Done()
			for i := 0; i < numRunsPerRoutines; i++ {
				index := numRunsPerRoutines*id + i
				result[index] = f(strconv.Itoa(index))
			}
		}(&wg, i, numRunsPerRoutines, result, f)
	}
	wg.Wait()

	sort.Slice(result, func(i, j int) bool {
		vi, _ := strconv.Atoi(result[i][1:])
		vj, _ := strconv.Atoi(result[j][1:])
		return vi < vj
	})
	for i, placeholder := range result {
		expected := ":" + strconv.Itoa(i+1)
		if placeholder != expected {
			t.Fatalf("%s failed: expected %#v at index %d but received %#v", testName, expected, i, result)
		}
	}
}

func TestNewPlaceholderGeneratorAtpiN(t *testing.T) {
	testName := "TestNewPlaceholderGeneratorAtpiN"
	f := NewPlaceholderGeneratorAtpiN()

	numRoutines := 8
	numRunsPerRoutines := 100
	result := make([]string, numRoutines*numRunsPerRoutines)
	var wg sync.WaitGroup
	wg.Add(numRoutines)
	for i := 0; i < numRoutines; i++ {
		go func(wg *sync.WaitGroup, id, numRunsPerRoutines int, result []string, f PlaceholderGenerator) {
			defer wg.Done()
			for i := 0; i < numRunsPerRoutines; i++ {
				index := numRunsPerRoutines*id + i
				result[index] = f(strconv.Itoa(index))
			}
		}(&wg, i, numRunsPerRoutines, result, f)
	}
	wg.Wait()

	sort.Slice(result, func(i, j int) bool {
		vi, _ := strconv.Atoi(result[i][2:])
		vj, _ := strconv.Atoi(result[j][2:])
		return vi < vj
	})
	for i, placeholder := range result {
		expected := "@p" + strconv.Itoa(i+1)
		if placeholder != expected {
			t.Fatalf("%s failed: expected %#v at index %d but received %#v", testName, expected, i, result)
		}
	}
}

func TestGenericSorting(t *testing.T) {
	testName := "TestGenericSorting"

	sorting := &GenericSorting{}
	if clause := sorting.Build(); clause != "" {
		t.Fatalf("%s failed: expected empty", testName)
	}

	flavorList := []sql.DbFlavor{sql.FlavorMySql, sql.FlavorPgSql, sql.FlavorMsSql, sql.FlavorOracle, sql.FlavorSqlite, sql.FlavorCosmosDb}
	for _, flavor := range flavorList {
		sorting = sorting.WithFlavor(flavor)
		if v := sorting.Flavor; flavor != v {
			t.Fatalf("%s failed: expected Flavor to be %#v but received %#v", testName, flavor, v)
		}
	}

	expected := "field0,field1 ASC,field2 DESC,field3 asc,field4 desc,field5"
	sorting = (&GenericSorting{}).Add("field0").Add("field1:1").Add("field2:-1").Add("field3:asc").Add("field4:desc").Add("field5")
	if clause := sorting.Build(); clause != expected {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v", testName, expected, clause)
	}
}

func TestGenericSorting_OptTableAlias(t *testing.T) {
	testName := "TestGenericSorting_OptTableAlias"

	sorting := &GenericSorting{}

	expected := "t.field0,t.field1 ASC,t.field2 DESC,t.field3 asc,t.field4 desc,t.field5"
	sorting = (&GenericSorting{}).Add("field0").Add("field1:1").Add("field2:-1").Add("field3:asc").Add("field4:desc").Add("field5")
	if clause := sorting.Build(OptTableAlias{TableAlias: "t"}); clause != expected {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v", testName, expected, clause)
	}

	expected = "c.field0,c.field1 ASC,c.field2 DESC,c.field3 asc,c.field4 desc,c.field5"
	sorting = (&GenericSorting{}).Add("field0").Add("field1:1").Add("field2:-1").Add("field3:asc").Add("field4:desc").Add("field5")
	if clause := sorting.Build(&OptTableAlias{TableAlias: "c"}); clause != expected {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v", testName, expected, clause)
	}
}

func TestGenericSorting_OptTableAliasFiltered(t *testing.T) {
	testName := "TestGenericSorting_OptTableAliasFiltered"

	sorting := &GenericSorting{}

	expected := "t0.field0,t1.field1 ASC,t2.field2 DESC,t.field3 asc,t.field4 desc,t.field5"
	sorting = (&GenericSorting{}).Add("t0.field0").Add("t1.field1:1").Add("t2.field2:-1").Add("field3:asc").Add("field4:desc").Add("field5")
	if clause := sorting.Build(OptTableAlias{TableAlias: "t"}); clause != expected {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v", testName, expected, clause)
	}
}

func TestFilterAndOr(t *testing.T) {
	testName := "TestFilterAndOr"

	filter := &FilterAndOr{}
	if clause, values := filter.Build(nil); clause != "" || len(values) != 0 {
		t.Fatalf("%s failed: expected empty/0 but received %#v/%#v", testName, clause, len(values))
	}
	filter.Add(&FilterAsIs{"field1 > 1"}).Add((&FilterAsIs{}).WithClause("field2 = a")).WithOperator("and")
	expected := "(field1 > 1) and (field2 = a)"
	if clause, values := filter.Build(nil); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}

	filter = &FilterAndOr{}
	filter.Add(&FilterFieldValue{Field: "field1", Operator: ">", Value: 1}).
		Add(&FilterFieldValue{Field: "field2", Operator: "=", Value: "a"})
	opList := []string{"and", "or", "||", "&&"}
	pgList := []PlaceholderGenerator{
		NewPlaceholderGeneratorQuestion(),
		NewPlaceholderGeneratorDollarN(),
		NewPlaceholderGeneratorColonN(),
		NewPlaceholderGeneratorAtpiN(),
	}
	expectedClauses := []string{
		"(field1 > ?) and (field2 = ?)",
		"(field1 > $1) or (field2 = $2)",
		"(field1 > :1) || (field2 = :2)",
		"(field1 > @p1) && (field2 = @p2)",
	}
	for i, pg := range pgList {
		filter.WithOperator(opList[i])
		expected = expectedClauses[i]
		if clause, values := filter.Build(pg); clause != expected || len(values) != 2 || values[0] != 1 || values[1] != "a" {
			t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
		}
	}
}

func TestFilterAndOr_OptTableAlias(t *testing.T) {
	testName := "TestFilterAndOr_OptTableAlias"
	filter := &FilterAndOr{}
	placeholderGenerator := NewPlaceholderGeneratorQuestion()
	filter.Add(&FilterFieldValue{Field: "field1", Operator: ">", Value: 1}).
		Add(&FilterFieldValue{Field: "field2", Operator: "=", Value: "a"}).
		WithOperator("OR")
	expected := "(t.field1 > ?) OR (t.field2 = ?)"
	if clause, values := filter.Build(placeholderGenerator, OptTableAlias{TableAlias: "t"}); clause != expected || len(values) != 2 || values[0] != 1 || values[1] != "a" {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	expected = "(c.field1 > ?) OR (c.field2 = ?)"
	if clause, values := filter.Build(placeholderGenerator, &OptTableAlias{TableAlias: "c"}); clause != expected || len(values) != 2 || values[0] != 1 || values[1] != "a" {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterAndOr_OptTableAliasFiltered(t *testing.T) {
	testName := "TestFilterAndOr_OptTableAliasFiltered"
	filter := &FilterAndOr{}
	placeholderGenerator := NewPlaceholderGeneratorQuestion()
	filter.Add(&FilterFieldValue{Field: "t0.field1", Operator: ">", Value: 1}).
		Add(&FilterFieldValue{Field: "field2", Operator: "=", Value: "a"}).
		WithOperator("OR")
	expected := "(t0.field1 > ?) OR (t.field2 = ?)"
	if clause, values := filter.Build(placeholderGenerator, OptTableAlias{TableAlias: "t"}); clause != expected || len(values) != 2 || values[0] != 1 || values[1] != "a" {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterAnd(t *testing.T) {
	testName := "TestFilterAnd"
	filter := &FilterAnd{}
	if clause, values := filter.Build(nil); clause != "" || len(values) != 0 {
		t.Fatalf("%s failed: expected empty/0 but received %#v/%#v", testName, clause, len(values))
	}

	opList := []string{"", "and", "&&", "&"}
	pgList := []PlaceholderGenerator{
		NewPlaceholderGeneratorQuestion(),
		NewPlaceholderGeneratorDollarN(),
		NewPlaceholderGeneratorColonN(),
		NewPlaceholderGeneratorAtpiN(),
	}
	expectedClauses := []string{
		"(field1 > ?) AND (field2 = ?)",
		"(field1 > $1) and (field2 = $2)",
		"(field1 > :1) && (field2 = :2)",
		"(field1 > @p1) & (field2 = @p2)",
	}
	filter = (&FilterAnd{}).Add(&FilterFieldValue{Field: "field1", Operator: ">", Value: 1}).
		Add(&FilterFieldValue{Field: "field2", Operator: "=", Value: "a"})
	for i, pg := range pgList {
		filter.WithOperator(opList[i])
		expected := expectedClauses[i]
		if clause, values := filter.Build(pg); clause != expected || len(values) != 2 || values[0] != 1 || values[1] != "a" {
			t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
		}
	}
}

func TestFilterAnd_OptTableAlias(t *testing.T) {
	testName := "TestFilterAnd_OptTableAlias"
	filter := (&FilterAnd{}).Add(&FilterFieldValue{Field: "field1", Operator: ">", Value: 1}).
		Add(&FilterFieldValue{Field: "field2", Operator: "=", Value: "a"})
	placeholderGenerator := NewPlaceholderGeneratorQuestion()
	expected := "(t.field1 > ?) AND (t.field2 = ?)"
	if clause, values := filter.Build(placeholderGenerator, OptTableAlias{TableAlias: "t"}); clause != expected || len(values) != 2 || values[0] != 1 || values[1] != "a" {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	expected = "(c.field1 > ?) AND (c.field2 = ?)"
	if clause, values := filter.Build(placeholderGenerator, &OptTableAlias{TableAlias: "c"}); clause != expected || len(values) != 2 || values[0] != 1 || values[1] != "a" {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterAnd_OptTableAliasFiltered(t *testing.T) {
	testName := "TestFilterAnd_OptTableAliasFiltered"
	filter := (&FilterAnd{}).Add(&FilterFieldValue{Field: "t0.field1", Operator: ">", Value: 1}).
		Add(&FilterFieldValue{Field: "field2", Operator: "=", Value: "a"})
	placeholderGenerator := NewPlaceholderGeneratorQuestion()
	expected := "(t0.field1 > ?) AND (t.field2 = ?)"
	if clause, values := filter.Build(placeholderGenerator, OptTableAlias{TableAlias: "t"}); clause != expected || len(values) != 2 || values[0] != 1 || values[1] != "a" {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterOr(t *testing.T) {
	testName := "TestFilterOr"
	filter := &FilterOr{}
	if clause, values := filter.Build(nil); clause != "" || len(values) != 0 {
		t.Fatalf("%s failed: expected empty/0 but received %#v/%#v", testName, clause, len(values))
	}

	opList := []string{"", "or", "||", "|"}
	pgList := []PlaceholderGenerator{
		NewPlaceholderGeneratorQuestion(),
		NewPlaceholderGeneratorDollarN(),
		NewPlaceholderGeneratorColonN(),
		NewPlaceholderGeneratorAtpiN(),
	}
	expectedClauses := []string{
		"(field1 > ?) OR (field2 = ?)",
		"(field1 > $1) or (field2 = $2)",
		"(field1 > :1) || (field2 = :2)",
		"(field1 > @p1) | (field2 = @p2)",
	}
	filter = (&FilterOr{}).Add(&FilterFieldValue{Field: "field1", Operator: ">", Value: 1}).
		Add(&FilterFieldValue{Field: "field2", Operator: "=", Value: "a"})
	for i, pg := range pgList {
		filter.WithOperator(opList[i])
		expected := expectedClauses[i]
		if clause, values := filter.Build(pg); clause != expected || len(values) != 2 || values[0] != 1 || values[1] != "a" {
			t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
		}
	}
}

func TestFilterOr_OptTableAlias(t *testing.T) {
	testName := "TestFilterOr_OptTableAlias"
	filter := (&FilterOr{}).Add(&FilterFieldValue{Field: "field1", Operator: ">", Value: 1}).
		Add(&FilterFieldValue{Field: "field2", Operator: "=", Value: "a"})
	placeholderGenerator := NewPlaceholderGeneratorQuestion()
	expected := "(t.field1 > ?) OR (t.field2 = ?)"
	if clause, values := filter.Build(placeholderGenerator, OptTableAlias{TableAlias: "t"}); clause != expected || len(values) != 2 || values[0] != 1 || values[1] != "a" {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	expected = "(c.field1 > ?) OR (c.field2 = ?)"
	if clause, values := filter.Build(placeholderGenerator, &OptTableAlias{TableAlias: "c"}); clause != expected || len(values) != 2 || values[0] != 1 || values[1] != "a" {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterOr_OptTableAliasFiltered(t *testing.T) {
	testName := "TestFilterOr_OptTableAliasFiltered"
	filter := (&FilterOr{}).Add(&FilterFieldValue{Field: "t0.field1", Operator: ">", Value: 1}).
		Add(&FilterFieldValue{Field: "field2", Operator: "=", Value: "a"})
	placeholderGenerator := NewPlaceholderGeneratorQuestion()
	expected := "(t0.field1 > ?) OR (t.field2 = ?)"
	if clause, values := filter.Build(placeholderGenerator, OptTableAlias{TableAlias: "t"}); clause != expected || len(values) != 2 || values[0] != 1 || values[1] != "a" {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterAsIs(t *testing.T) {
	testName := "TestFilterAsIs"
	filter := &FilterAsIs{}
	expected := "my clause"
	filter.WithClause("my clause")
	if clause, values := filter.Build(nil); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	if clause, values := filter.Build(nil, &OptTableAlias{"a"}); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion()); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion(), OptTableAlias{"b"}); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterBetween(t *testing.T) {
	testName := "TestFilterBetween"
	filter := &FilterBetween{}
	for _, field := range []string{"field1", "field2", "field3"} {
		filter = filter.WithField(field)
		if v := filter.Field; field != v {
			t.Fatalf("%s failed: expected Field to be %#v but received %#v", testName, field, v)
		}
	}
	for _, op := range []string{"IN", "BETWEEN"} {
		filter = filter.WithOperator(op)
		if v := filter.Operator; op != v {
			t.Fatalf("%s failed: expected Operator to be %#v but received %#v", testName, op, v)
		}
	}
	for _, value := range []interface{}{true, 0, 1.2, "3"} {
		filter = filter.WithLeftValue(value)
		if v := filter.ValueLeft; value != v {
			t.Fatalf("%s failed: expected ValueLeft to be %#v but received %#v", testName, value, v)
		}
	}
	for _, value := range []interface{}{"true", 0.1, 2, false} {
		filter = filter.WithRightValue(value)
		if v := filter.ValueRight; value != v {
			t.Fatalf("%s failed: expected ValueRight to be %#v but received %#v", testName, value, v)
		}
	}

	filter = &FilterBetween{Field: "myfield", ValueLeft: 0, ValueRight: 9}
	if clause, values := filter.Build(nil); clause != "" || len(values) != 0 {
		t.Fatalf("%s failed: expected empty/0 but received %#v/%#v", testName, clause, len(values))
	}

	opList := []string{"", "IN", "between", "Range"}
	pgList := []PlaceholderGenerator{
		NewPlaceholderGeneratorQuestion(),
		NewPlaceholderGeneratorDollarN(),
		NewPlaceholderGeneratorColonN(),
		NewPlaceholderGeneratorAtpiN(),
	}
	expectedClauses := []string{
		"myfield BETWEEN ? AND ?",
		"myfield IN $1 AND $2",
		"myfield between :1 AND :2",
		"myfield Range @p1 AND @p2",
	}
	for i, pg := range pgList {
		filter.WithOperator(opList[i])
		expected := expectedClauses[i]
		if clause, values := filter.Build(pg); clause != expected || len(values) != 2 {
			t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
		}
	}
}

func TestFilterBetween_CustomGenerator(t *testing.T) {
	testName := "TestFilterBetween_CustomGenerator"
	filter := &FilterBetween{Field: "myfield", ValueLeft: 0, ValueRight: 9}
	var customGenerator StmGeneratorBetween = func(pg PlaceholderGenerator, field string, leftValue, rightValue interface{}, opts ...interface{}) (string, []interface{}) {
		if pg == nil {
			return "", []interface{}{}
		}
		tableAlias := extractOptTableAlias(opts...)
		if reColnamePrefixedTblname.MatchString(field) {
			tableAlias = ""
		}
		values := []interface{}{leftValue, rightValue}
		clause := fmt.Sprintf("(%s <= %s%s < %s)", pg(field), tableAlias, field, pg(field))
		return clause, values
	}
	if clause, values := filter.Build(nil, OptBetweenGenerator{customGenerator}); clause != "" || len(values) != 0 {
		t.Fatalf("%s failed: expected empty/0 but received %#v/%#v", testName, clause, len(values))
	}

	opList := []string{"", "IN", "between", "Range"}
	pgList := []PlaceholderGenerator{
		NewPlaceholderGeneratorQuestion(),
		NewPlaceholderGeneratorDollarN(),
		NewPlaceholderGeneratorColonN(),
		NewPlaceholderGeneratorAtpiN(),
	}
	expectedClauses := []string{
		"(? <= myfield < ?)",
		"($1 <= myfield < $2)",
		"(:1 <= myfield < :2)",
		"(@p1 <= myfield < @p2)",
	}
	for i, pg := range pgList {
		filter.WithOperator(opList[i])
		expected := expectedClauses[i]
		var opt interface{} = OptBetweenGenerator{customGenerator}
		if i%2 == 0 {
			opt = &OptBetweenGenerator{customGenerator}
		}
		if clause, values := filter.Build(pg, opt); clause != expected || len(values) != 2 {
			fmt.Printf("%T\n", opt)
			t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
		}
	}

	expected := "(? <= t.myfield < ?)"
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion(), OptBetweenGenerator{customGenerator}, OptTableAlias{"t"}); clause != expected || len(values) != 2 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	expected = "(? <= c.myfield < ?)"
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion(), &OptTableAlias{"c"}, &OptBetweenGenerator{customGenerator}); clause != expected || len(values) != 2 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}

	filter.WithField("t0.myfield")
	expected = "(? <= t0.myfield < ?)"
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion(), &OptTableAlias{"c"}, &OptBetweenGenerator{customGenerator}); clause != expected || len(values) != 2 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterBetween_OptTableAlias(t *testing.T) {
	testName := "TestFilterBetween_OptTableAlias"
	filter := &FilterBetween{Field: "myfield", ValueLeft: 0, ValueRight: 9}
	expected := "t.myfield BETWEEN ? AND ?"
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion(), OptTableAlias{TableAlias: "t"}); clause != expected || len(values) != 2 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	expected = "c.myfield BETWEEN ? AND ?"
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion(), &OptTableAlias{TableAlias: "c"}); clause != expected || len(values) != 2 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterBetween_OptTableAliasFiltered(t *testing.T) {
	testName := "TestFilterBetween_OptTableAliasFiltered"
	filter := &FilterBetween{Field: "t0.myfield", ValueLeft: 0, ValueRight: 9}
	expected := "t0.myfield BETWEEN ? AND ?"
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion(), OptTableAlias{TableAlias: "t"}); clause != expected || len(values) != 2 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterFieldValue(t *testing.T) {
	testName := "TestFilterFieldValue"
	filter := &FilterFieldValue{}
	for _, field := range []string{"field1", "field2", "field3"} {
		filter = filter.WithField(field)
		if v := filter.Field; field != v {
			t.Fatalf("%s failed: expected Field to be %#v but received %#v", testName, field, v)
		}
	}
	for _, op := range []string{"LT", "GT", "EQ", "LTE", "GTE"} {
		filter = filter.WithOperator(op)
		if v := filter.Operator; op != v {
			t.Fatalf("%s failed: expected Operator to be %#v but received %#v", testName, op, v)
		}
	}
	for _, value := range []interface{}{true, 0, 1.2, "3"} {
		filter = filter.WithValue(value)
		if v := filter.Value; value != v {
			t.Fatalf("%s failed: expected Value to be %#v but received %#v", testName, value, v)
		}
	}

	filter = &FilterFieldValue{Field: "myfield", Operator: "=", Value: -1}
	if clause, values := filter.Build(nil); clause != "" || len(values) != 0 {
		t.Fatalf("%s failed: expected empty/0 but received %#v/%#v", testName, clause, len(values))
	}

	opList := []string{"=", ">", "<", "!="}
	pgList := []PlaceholderGenerator{
		NewPlaceholderGeneratorQuestion(),
		NewPlaceholderGeneratorDollarN(),
		NewPlaceholderGeneratorColonN(),
		NewPlaceholderGeneratorAtpiN(),
	}
	expectedClauses := []string{
		"myfield = ?",
		"myfield > $1",
		"myfield < :1",
		"myfield != @p1",
	}
	for i, pg := range pgList {
		filter.WithOperator(opList[i])
		expected := expectedClauses[i]
		if clause, values := filter.Build(pg); clause != expected || len(values) != 1 || values[0] != -1 {
			t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
		}
	}
}

func TestFilterFieldValue_OptTableAlias(t *testing.T) {
	testName := "TestFilterFieldValue_OptTableAlias"
	filter := &FilterFieldValue{Field: "myfield", Operator: "=", Value: 0}
	placeholderGenerator := NewPlaceholderGeneratorQuestion()
	expected := "t.myfield = ?"
	if clause, values := filter.Build(placeholderGenerator, OptTableAlias{TableAlias: "t"}); clause != expected || len(values) != 1 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	expected = "c.myfield = ?"
	if clause, values := filter.Build(placeholderGenerator, &OptTableAlias{TableAlias: "c"}); clause != expected || len(values) != 1 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterFieldValue_OptTableAliasFiltered(t *testing.T) {
	testName := "TestFilterFieldValue_OptTableAliasFiltered"
	filter := &FilterFieldValue{Field: "t0.myfield", Operator: "=", Value: 0}
	placeholderGenerator := NewPlaceholderGeneratorQuestion()
	expected := "t0.myfield = ?"
	if clause, values := filter.Build(placeholderGenerator, OptTableAlias{TableAlias: "t"}); clause != expected || len(values) != 1 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterIsNull(t *testing.T) {
	testName := "TestFilterIsNull"
	filter := &FilterIsNull{}
	for _, field := range []string{"field1", "field2", "field3"} {
		filter = filter.WithField(field)
		if v := filter.Field; field != v {
			t.Fatalf("%s failed: expected Field to be %#v but received %#v", testName, field, v)
		}
	}
	for _, op := range []string{"IS", "=", "=="} {
		filter = filter.WithOperator(op)
		if v := filter.Operator; op != v {
			t.Fatalf("%s failed: expected Operator to be %#v but received %#v", testName, op, v)
		}
	}

	filter = (&FilterIsNull{}).WithField("myfield")
	expected := "myfield IS NULL"
	if clause, values := filter.Build(nil); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion()); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	expected = "myfield = null"
	if clause, values := filter.Build(nil, OptDbFlavor{sql.FlavorCosmosDb}); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion(), &OptDbFlavor{sql.FlavorCosmosDb}); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterIsNull_OptTableAlias(t *testing.T) {
	testName := "TestFilterIsNull_OptTableAlias"
	filter := (&FilterIsNull{}).WithField("myfield")
	expected := "t.myfield IS NULL"
	if clause, values := filter.Build(nil, OptTableAlias{"t"}); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	expected = "c.myfield = null"
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion(), OptDbFlavor{sql.FlavorCosmosDb}, &OptTableAlias{"c"}); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterIsNull_OptTableAliasFiltered(t *testing.T) {
	testName := "TestFilterIsNull_OptTableAliasFiltered"
	filter := (&FilterIsNull{}).WithField("t0.myfield")
	expected := "t0.myfield IS NULL"
	if clause, values := filter.Build(nil, OptTableAlias{"t"}); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	expected = "t0.myfield = null"
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion(), OptDbFlavor{sql.FlavorCosmosDb}, &OptTableAlias{"c"}); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterIsNotNull(t *testing.T) {
	testName := "TestFilterIsNotNull"
	filter := &FilterIsNotNull{}
	for _, field := range []string{"field1", "field2", "field3"} {
		filter = filter.WithField(field)
		if v := filter.Field; field != v {
			t.Fatalf("%s failed: expected Field to be %#v but received %#v", testName, field, v)
		}
	}
	for _, op := range []string{"IS", "=", "=="} {
		filter = filter.WithOperator(op)
		if v := filter.Operator; op != v {
			t.Fatalf("%s failed: expected Operator to be %#v but received %#v", testName, op, v)
		}
	}

	filter = (&FilterIsNotNull{}).WithField("myfield")
	expected := "myfield IS NOT NULL"
	if clause, values := filter.Build(nil); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion()); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	expected = "myfield != null"
	if clause, values := filter.Build(nil, OptDbFlavor{sql.FlavorCosmosDb}); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion(), &OptDbFlavor{sql.FlavorCosmosDb}); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterIsNotNull_OptTableAlias(t *testing.T) {
	testName := "TestFilterIsNotNull_OptTableAlias"
	filter := (&FilterIsNotNull{}).WithField("myfield")
	expected := "t.myfield IS NOT NULL"
	if clause, values := filter.Build(nil, OptTableAlias{"t"}); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	expected = "c.myfield != null"
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion(), OptDbFlavor{sql.FlavorCosmosDb}, &OptTableAlias{"c"}); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterIsNotNull_OptTableAliasFiltered(t *testing.T) {
	testName := "TestFilterIsNotNull_OptTableAliasFiltered"
	filter := (&FilterIsNotNull{}).WithField("t0.myfield")
	expected := "t0.myfield IS NOT NULL"
	if clause, values := filter.Build(nil, OptTableAlias{"t"}); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	expected = "t0.myfield != null"
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion(), OptDbFlavor{sql.FlavorCosmosDb}, &OptTableAlias{"c"}); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterExpression(t *testing.T) {
	testName := "TestFilterExpression"
	filter := &FilterExpression{}
	for _, field := range []string{"field1", "field2", "field3"} {
		filter = filter.WithLeft(field)
		if v := filter.Left; field != v {
			t.Fatalf("%s failed: expected Left to be %#v but received %#v", testName, field, v)
		}
	}
	for _, field := range []string{"field4", "field5", "field6"} {
		filter = filter.WithRight(field)
		if v := filter.Right; field != v {
			t.Fatalf("%s failed: expected Right to be %#v but received %#v", testName, field, v)
		}
	}
	for _, op := range []string{"=", "!=", "<>", "<", "<=", ">", ">="} {
		filter = filter.WithOperator(op)
		if v := filter.Operator; op != v {
			t.Fatalf("%s failed: expected Operator to be %#v but received %#v", testName, op, v)
		}
	}

	filter = &FilterExpression{Left: "field1", Operator: "=", Right: "field2"}
	expected := "field1 = field2"
	if clause, values := filter.Build(nil); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion()); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion(), OptDbFlavor{sql.FlavorMySql}); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterExpression_OptTableAlias(t *testing.T) {
	testName := "TestFilterExpression_OptTableAlias"
	filter := &FilterExpression{Left: "field1", Operator: "=", Right: "field2"}
	expected := "t.field1 = t.field2"
	if clause, values := filter.Build(nil, OptTableAlias{"t"}); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
	expected = "c.field1 = c.field2"
	if clause, values := filter.Build(NewPlaceholderGeneratorQuestion(), &OptTableAlias{"c"}); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestFilterExpression_OptTableAliasFiltered(t *testing.T) {
	testName := "TestFilterExpression_OptTableAliasFiltered"
	filter := &FilterExpression{Left: "t0.field1", Operator: "=", Right: "field2"}
	expected := "t0.field1 = t.field2"
	if clause, values := filter.Build(nil, OptTableAlias{"t"}); clause != expected || len(values) != 0 {
		t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, clause, values)
	}
}

func TestBaseSqlBuilder(t *testing.T) {
	testName := "TestBaseSqlBuilder"
	builder := &BaseSqlBuilder{}
	flavorList := []sql.DbFlavor{sql.FlavorDefault, sql.FlavorMySql, sql.FlavorPgSql, sql.FlavorMsSql, sql.FlavorOracle, sql.FlavorSqlite, sql.FlavorCosmosDb}
	for _, flavor := range flavorList {
		builder = builder.WithFlavor(flavor)
		if v := builder.Flavor; v != flavor {
			t.Fatalf("%s failed: expected Flavor to be %#v but received %#v", testName, flavor, v)
		}
	}
	for _, table := range []string{"table1", "table2", "table3"} {
		builder = builder.WithTable(table)
		if v := builder.Table; v != table {
			t.Fatalf("%s failed: expected Table to be %#v but received %#v", testName, table, v)
		}
	}
	for _, pg := range []PlaceholderGenerator{NewPlaceholderGeneratorQuestion(), nil, NewPlaceholderGeneratorColonN(), NewPlaceholderGeneratorAtpiN(), NewPlaceholderGeneratorDollarN()} {
		builder = builder.WithPlaceholderGenerator(pg)
		if v := builder.PlaceholderGenerator; fmt.Sprintf("%p", pg) != fmt.Sprintf("%p", v) {
			t.Fatalf("%s failed: expected PlaceholderGenerator to be %p but received %p", testName, pg, v)
		}
	}

	clone := builder.Clone()
	if fmt.Sprintf("%p", clone.PlaceholderGenerator) != fmt.Sprintf("%p", builder.PlaceholderGenerator) ||
		clone.Flavor != builder.Flavor || clone.Table != builder.Table {
		t.Fatalf("%s failed: Clone", testName)
	}
	clone.Table += "-new"
	clone.Flavor = sql.FlavorDefault
	if clone.Table == builder.Table || clone.Flavor == builder.Flavor {
		t.Fatalf("%s failed: clone {%s - %d - %p} / original {%s - %d - %p}", testName, clone.Table, clone.Flavor, clone.PlaceholderGenerator, builder.Table, builder.Flavor, builder.PlaceholderGenerator)
	}
}

func TestDeleteBuilder(t *testing.T) {
	testName := "TestDeleteBuilder"
	flavorList := []sql.DbFlavor{sql.FlavorDefault, sql.FlavorMySql, sql.FlavorPgSql, sql.FlavorMsSql, sql.FlavorOracle, sql.FlavorSqlite, sql.FlavorCosmosDb}
	expectedSql := map[sql.DbFlavor]string{
		sql.FlavorPgSql:    "DELETE FROM mytable WHERE (field1 > $1) AND (field2 = $2)",
		sql.FlavorCosmosDb: "DELETE FROM mytable WHERE (field1 > $1) AND (field2 = $2)",
		sql.FlavorMsSql:    "DELETE FROM mytable WHERE (field1 > @p1) AND (field2 = @p2)",
		sql.FlavorOracle:   "DELETE FROM mytable WHERE (field1 > :1) AND (field2 = :2)",
		sql.FlavorMySql:    "DELETE FROM mytable WHERE (field1 > ?) AND (field2 = ?)",
		sql.FlavorSqlite:   "DELETE FROM mytable WHERE (field1 > ?) AND (field2 = ?)",
		sql.FlavorDefault:  "DELETE FROM mytable WHERE (field1 > ?) AND (field2 = ?)",
	}
	for _, flavor := range flavorList {
		// note: .WithFlavor also correctly sets PlaceholderGenerator
		builder := NewDeleteBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).WithTable("mytable")

		expected := "DELETE FROM mytable"
		if sql, values := builder.Build(); sql != expected || len(values) != 0 {
			t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, sql, values)
		}

		filter := (&FilterAnd{}).Add(&FilterFieldValue{Field: "field1", Operator: ">", Value: 1}).
			Add((&FilterFieldValue{}).WithField("field2").WithOperator("=").WithValue("a"))
		builder.WithFilter(filter)
		// note: .Build uses its internal DbFlavor, not the one supplied via parameters
		if sql, values := builder.Build(OptDbFlavor{sql.FlavorDefault}); sql != expectedSql[flavor] || len(values) != 2 || values[0] != 1 || values[1] != "a" {
			t.Fatalf("%s failed: (%#v)\nexpected: %#v\nreceived: %#v / %#v", testName, flavor, expectedSql[flavor], sql, values)
		}
	}
}

func TestDeleteBuilder_OptTableAlias(t *testing.T) {
	testName := "TestDeleteBuilder_OptTableAlias"
	flavorList := []sql.DbFlavor{sql.FlavorDefault, sql.FlavorMySql, sql.FlavorPgSql, sql.FlavorMsSql, sql.FlavorOracle, sql.FlavorSqlite, sql.FlavorCosmosDb}
	expectedSql := map[sql.DbFlavor]string{
		sql.FlavorPgSql:    "DELETE FROM mytable t WHERE (t.field1 > $1) OR (t.field2 = $2)",
		sql.FlavorCosmosDb: "DELETE FROM mytable WHERE (field1 > $1) OR (field2 = $2)",
		sql.FlavorMsSql:    "DELETE FROM mytable t WHERE (t.field1 > @p1) OR (t.field2 = @p2)",
		sql.FlavorOracle:   "DELETE FROM mytable t WHERE (t.field1 > :1) OR (t.field2 = :2)",
		sql.FlavorMySql:    "DELETE FROM mytable t WHERE (t.field1 > ?) OR (t.field2 = ?)",
		sql.FlavorSqlite:   "DELETE FROM mytable t WHERE (t.field1 > ?) OR (t.field2 = ?)",
		sql.FlavorDefault:  "DELETE FROM mytable t WHERE (t.field1 > ?) OR (t.field2 = ?)",
	}
	for _, flavor := range flavorList {
		// note: .WithFlavor also correctly sets PlaceholderGenerator
		builder := NewDeleteBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).WithTable("mytable")

		expected := "DELETE FROM mytable t"
		if flavor == sql.FlavorCosmosDb {
			expected = "DELETE FROM mytable"
		}
		if sql, values := builder.Build(&OptTableAlias{"t"}); sql != expected || len(values) != 0 {
			t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName, expected, sql, values)
		}

		filter := (&FilterOr{}).Add(&FilterFieldValue{Field: "field1", Operator: ">", Value: 1}).
			Add((&FilterFieldValue{}).WithField("field2").WithOperator("=").WithValue("a"))
		builder.WithFilter(filter)
		if sql, values := builder.Build(OptTableAlias{"t"}); sql != expectedSql[flavor] || len(values) != 2 || values[0] != 1 || values[1] != "a" {
			t.Fatalf("%s failed: (%#v)\nexpected: %#v\nreceived: %#v / %#v", testName, flavor, expectedSql[flavor], sql, values)
		}
	}
}

func TestDeleteBuilder_OptTableAliasFiltered(t *testing.T) {
	testName := "TestDeleteBuilder_OptTableAliasFiltered"
	flavorList := []sql.DbFlavor{sql.FlavorDefault, sql.FlavorMySql, sql.FlavorPgSql, sql.FlavorMsSql, sql.FlavorOracle, sql.FlavorSqlite, sql.FlavorCosmosDb}
	expectedSql := map[sql.DbFlavor]string{
		sql.FlavorPgSql:    "DELETE FROM mytable t0 WHERE (t0.field1 > $1) OR (t.field2 = $2)",
		sql.FlavorCosmosDb: "DELETE FROM mytable t0 WHERE (t0.field1 > $1) OR (field2 = $2)",
		sql.FlavorMsSql:    "DELETE FROM mytable t0 WHERE (t0.field1 > @p1) OR (t.field2 = @p2)",
		sql.FlavorOracle:   "DELETE FROM mytable t0 WHERE (t0.field1 > :1) OR (t.field2 = :2)",
		sql.FlavorMySql:    "DELETE FROM mytable t0 WHERE (t0.field1 > ?) OR (t.field2 = ?)",
		sql.FlavorSqlite:   "DELETE FROM mytable t0 WHERE (t0.field1 > ?) OR (t.field2 = ?)",
		sql.FlavorDefault:  "DELETE FROM mytable t0 WHERE (t0.field1 > ?) OR (t.field2 = ?)",
	}
	for _, flavor := range flavorList {
		// note: .WithFlavor also correctly sets PlaceholderGenerator
		builder := NewDeleteBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).WithTable("mytable t0")

		expected := "DELETE FROM mytable t0"
		if sql, values := builder.Build(&OptTableAlias{"t"}); sql != expected || len(values) != 0 {
			t.Fatalf("%s failed:\nexpected: %#v\nreceived: %#v / %#v", testName+"/"+strconv.Itoa(int(flavor)), expected, sql, values)
		}

		filter := (&FilterOr{}).Add(&FilterFieldValue{Field: "t0.field1", Operator: ">", Value: 1}).
			Add((&FilterFieldValue{}).WithField("field2").WithOperator("=").WithValue("a"))
		builder.WithFilter(filter)
		if sql, values := builder.Build(OptTableAlias{"t"}); sql != expectedSql[flavor] || len(values) != 2 || values[0] != 1 || values[1] != "a" {
			t.Fatalf("%s failed: (%#v)\nexpected: %#v\nreceived: %#v / %#v", testName+"/"+strconv.Itoa(int(flavor)), flavor, expectedSql[flavor], sql, values)
		}
	}
}

func TestInsertBuilder(t *testing.T) {
	testName := "TestInsertBuilder"
	flavorList := []sql.DbFlavor{sql.FlavorDefault, sql.FlavorMySql, sql.FlavorPgSql, sql.FlavorMsSql, sql.FlavorOracle, sql.FlavorSqlite, sql.FlavorCosmosDb}
	expectedSql := map[sql.DbFlavor]string{
		sql.FlavorPgSql:    "INSERT INTO mytable (field1,field2) VALUES ($1,$2)",
		sql.FlavorCosmosDb: "INSERT INTO mytable (field1,field2) VALUES ($1,$2)",
		sql.FlavorMsSql:    "INSERT INTO mytable (field1,field2) VALUES (@p1,@p2)",
		sql.FlavorOracle:   "INSERT INTO mytable (field1,field2) VALUES (:1,:2)",
		sql.FlavorMySql:    "INSERT INTO mytable (field1,field2) VALUES (?,?)",
		sql.FlavorSqlite:   "INSERT INTO mytable (field1,field2) VALUES (?,?)",
		sql.FlavorDefault:  "INSERT INTO mytable (field1,field2) VALUES (?,?)",
	}
	for _, flavor := range flavorList {
		// note: .WithFlavor also correctly sets PlaceholderGenerator
		builder := NewInsertBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).
			WithTable("mytable").AddValues(map[string]interface{}{"fieldx": 2, "fieldy": "b"}).
			WithValues(map[string]interface{}{"field1": 1, "field2": "a"})
		// note: .Build uses its internal DbFlavor, not the one supplied via parameters
		if sql, values := builder.Build(OptDbFlavor{sql.FlavorDefault}); sql != expectedSql[flavor] || len(values) != 2 || values[0] != 1 || values[1] != "a" {
			t.Fatalf("%s failed: (%#v)\nexpected: %#v\nreceived: %#v / %#v", testName+"/"+strconv.Itoa(int(flavor)), flavor, expectedSql[flavor], sql, values)
		}
	}
}

func TestInsertBuilder_OptTableAlias(t *testing.T) {
	testName := "TestInsertBuilder_OptTableAlias"
	flavorList := []sql.DbFlavor{sql.FlavorDefault, sql.FlavorMySql, sql.FlavorPgSql, sql.FlavorMsSql, sql.FlavorOracle, sql.FlavorSqlite, sql.FlavorCosmosDb}
	expectedSql := map[sql.DbFlavor]string{
		sql.FlavorPgSql:    "INSERT INTO mytable t (t.field1,t.field2) VALUES ($1,$2)",
		sql.FlavorCosmosDb: "INSERT INTO mytable (field1,field2) VALUES ($1,$2)",
		sql.FlavorMsSql:    "INSERT INTO mytable t (t.field1,t.field2) VALUES (@p1,@p2)",
		sql.FlavorOracle:   "INSERT INTO mytable t (t.field1,t.field2) VALUES (:1,:2)",
		sql.FlavorMySql:    "INSERT INTO mytable t (t.field1,t.field2) VALUES (?,?)",
		sql.FlavorSqlite:   "INSERT INTO mytable t (t.field1,t.field2) VALUES (?,?)",
		sql.FlavorDefault:  "INSERT INTO mytable t (t.field1,t.field2) VALUES (?,?)",
	}
	for _, flavor := range flavorList {
		// note: .WithFlavor also correctly sets PlaceholderGenerator
		builder := NewInsertBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).
			WithTable("mytable").WithValues(map[string]interface{}{"field1": 1, "field2": "a"})
		if sql, values := builder.Build(&OptTableAlias{"t"}); sql != expectedSql[flavor] || len(values) != 2 || values[0] != 1 || values[1] != "a" {
			t.Fatalf("%s failed: (%#v)\nexpected: %#v\nreceived: %#v / %#v", testName+"/"+strconv.Itoa(int(flavor)), flavor, expectedSql[flavor], sql, values)
		}
	}
}

func TestInsertBuilder_OptTableAliasFiltered(t *testing.T) {
	testName := "TestInsertBuilder_OptTableAliasFiltered"
	flavorList := []sql.DbFlavor{sql.FlavorDefault, sql.FlavorMySql, sql.FlavorPgSql, sql.FlavorMsSql, sql.FlavorOracle, sql.FlavorSqlite, sql.FlavorCosmosDb}
	expectedSql := map[sql.DbFlavor]string{
		sql.FlavorPgSql:    "INSERT INTO mytable t0 (t.field1,t0.field2) VALUES ($1,$2)",
		sql.FlavorCosmosDb: "INSERT INTO mytable t0 (field1,t0.field2) VALUES ($1,$2)",
		sql.FlavorMsSql:    "INSERT INTO mytable t0 (t.field1,t0.field2) VALUES (@p1,@p2)",
		sql.FlavorOracle:   "INSERT INTO mytable t0 (t.field1,t0.field2) VALUES (:1,:2)",
		sql.FlavorMySql:    "INSERT INTO mytable t0 (t.field1,t0.field2) VALUES (?,?)",
		sql.FlavorSqlite:   "INSERT INTO mytable t0 (t.field1,t0.field2) VALUES (?,?)",
		sql.FlavorDefault:  "INSERT INTO mytable t0 (t.field1,t0.field2) VALUES (?,?)",
	}
	for _, flavor := range flavorList {
		// note: .WithFlavor also correctly sets PlaceholderGenerator
		builder := NewInsertBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).
			WithTable("mytable t0").WithValues(map[string]interface{}{"field1": 1, "t0.field2": "a"})
		if sql, values := builder.Build(OptTableAlias{"t"}); sql != expectedSql[flavor] || len(values) != 2 || values[0] != 1 || values[1] != "a" {
			t.Fatalf("%s failed: (%#v)\nexpected: %#v\nreceived: %#v / %#v", testName+"/"+strconv.Itoa(int(flavor)), flavor, expectedSql[flavor], sql, values)
		}
	}
}

func TestUpdateBuilder_NoFilter(t *testing.T) {
	testName := "TestUpdateBuilder_NoFilter"
	flavorList := []sql.DbFlavor{sql.FlavorDefault, sql.FlavorMySql, sql.FlavorPgSql, sql.FlavorMsSql, sql.FlavorOracle, sql.FlavorSqlite, sql.FlavorCosmosDb}
	expectedSql := map[sql.DbFlavor]string{
		sql.FlavorPgSql:    "UPDATE mytable SET field1=$1,field2=$2",
		sql.FlavorCosmosDb: "UPDATE mytable SET field1=$1,field2=$2",
		sql.FlavorMsSql:    "UPDATE mytable SET field1=@p1,field2=@p2",
		sql.FlavorOracle:   "UPDATE mytable SET field1=:1,field2=:2",
		sql.FlavorMySql:    "UPDATE mytable SET field1=?,field2=?",
		sql.FlavorSqlite:   "UPDATE mytable SET field1=?,field2=?",
		sql.FlavorDefault:  "UPDATE mytable SET field1=?,field2=?",
	}
	for _, flavor := range flavorList {
		// note: .WithFlavor also correctly sets PlaceholderGenerator
		builder := NewUpdateBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).WithTable("mytable")
		builder.AddValues(map[string]interface{}{"1": 1, "2": "two"})
		if !reflect.DeepEqual(builder.Values, map[string]interface{}{"1": 1, "2": "two"}) {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, map[string]interface{}{"1": 1, "2": "two"}, builder.Values)
		}
		builder.WithValues(map[string]interface{}{"field1": 1, "field2": "a"})
		if !reflect.DeepEqual(builder.Values, map[string]interface{}{"field1": 1, "field2": "a"}) {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, map[string]interface{}{"field1": 1, "field2": "a"}, builder.Values)
		}
		// note: .Build uses its internal DbFlavor, not the one supplied via parameters
		if sql, values := builder.Build(OptDbFlavor{sql.FlavorDefault}); sql != expectedSql[flavor] || len(values) != 2 || values[0] != 1 || values[1] != "a" {
			t.Fatalf("%s failed: (%#v)\nexpected: %#v\nreceived: %#v / %#v", testName, flavor, expectedSql[flavor], sql, values)
		}
	}
}

func TestUpdateBuilder_NoFilter_OptDbAlias(t *testing.T) {
	testName := "TestUpdateBuilder_NoFilter_OptDbAlias"
	flavorList := []sql.DbFlavor{sql.FlavorDefault, sql.FlavorMySql, sql.FlavorPgSql, sql.FlavorMsSql, sql.FlavorOracle, sql.FlavorSqlite, sql.FlavorCosmosDb}
	expectedSql := map[sql.DbFlavor]string{
		sql.FlavorPgSql:    "UPDATE mytable t SET t.field1=$1,t.field2=$2",
		sql.FlavorCosmosDb: "UPDATE mytable SET field1=$1,field2=$2",
		sql.FlavorMsSql:    "UPDATE mytable t SET t.field1=@p1,t.field2=@p2",
		sql.FlavorOracle:   "UPDATE mytable t SET t.field1=:1,t.field2=:2",
		sql.FlavorMySql:    "UPDATE mytable t SET t.field1=?,t.field2=?",
		sql.FlavorSqlite:   "UPDATE mytable t SET t.field1=?,t.field2=?",
		sql.FlavorDefault:  "UPDATE mytable t SET t.field1=?,t.field2=?",
	}
	for _, flavor := range flavorList {
		// note: .WithFlavor also correctly sets PlaceholderGenerator
		builder := NewUpdateBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).WithTable("mytable")
		builder.WithValues(map[string]interface{}{"field1": 1, "field2": "a"})
		if sql, values := builder.Build(OptTableAlias{"t"}); sql != expectedSql[flavor] || len(values) != 2 || values[0] != 1 || values[1] != "a" {
			t.Fatalf("%s failed: (%#v)\nexpected: %#v\nreceived: %#v / %#v", testName, flavor, expectedSql[flavor], sql, values)
		}
	}
}

func TestUpdateBuilder_NoFilter_OptDbAliasFiltered(t *testing.T) {
	testName := "TestUpdateBuilder_NoFilter_OptDbAliasFiltered"
	flavorList := []sql.DbFlavor{sql.FlavorDefault, sql.FlavorMySql, sql.FlavorPgSql, sql.FlavorMsSql, sql.FlavorOracle, sql.FlavorSqlite, sql.FlavorCosmosDb}
	expectedSql := map[sql.DbFlavor]string{
		sql.FlavorPgSql:    "UPDATE mytable t0 SET t.field1=$1,t1.field2=$2",
		sql.FlavorCosmosDb: "UPDATE mytable t0 SET field1=$1,t1.field2=$2",
		sql.FlavorMsSql:    "UPDATE mytable t0 SET t.field1=@p1,t1.field2=@p2",
		sql.FlavorOracle:   "UPDATE mytable t0 SET t.field1=:1,t1.field2=:2",
		sql.FlavorMySql:    "UPDATE mytable t0 SET t.field1=?,t1.field2=?",
		sql.FlavorSqlite:   "UPDATE mytable t0 SET t.field1=?,t1.field2=?",
		sql.FlavorDefault:  "UPDATE mytable t0 SET t.field1=?,t1.field2=?",
	}
	for _, flavor := range flavorList {
		// note: .WithFlavor also correctly sets PlaceholderGenerator
		builder := NewUpdateBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).WithTable("mytable t0")
		builder.WithValues(map[string]interface{}{"field1": 1, "t1.field2": "a"})
		if sql, values := builder.Build(&OptTableAlias{"t"}); sql != expectedSql[flavor] || len(values) != 2 || values[0] != 1 || values[1] != "a" {
			t.Fatalf("%s failed: (%#v)\nexpected: %#v\nreceived: %#v / %#v", testName, flavor, expectedSql[flavor], sql, values)
		}
	}
}

func TestUpdateBuilder_WithFilter(t *testing.T) {
	testName := "TestUpdateBuilder_WithFilter"
	flavorList := []sql.DbFlavor{sql.FlavorDefault, sql.FlavorMySql, sql.FlavorPgSql, sql.FlavorMsSql, sql.FlavorOracle, sql.FlavorSqlite, sql.FlavorCosmosDb}
	expectedSql := map[sql.DbFlavor]string{
		sql.FlavorPgSql:    "UPDATE mytable SET field1=$1,field2=$2 WHERE (field1 > $3) AND (field2 = $4)",
		sql.FlavorCosmosDb: "UPDATE mytable SET field1=$1,field2=$2 WHERE (field1 > $3) AND (field2 = $4)",
		sql.FlavorMsSql:    "UPDATE mytable SET field1=@p1,field2=@p2 WHERE (field1 > @p3) AND (field2 = @p4)",
		sql.FlavorOracle:   "UPDATE mytable SET field1=:1,field2=:2 WHERE (field1 > :3) AND (field2 = :4)",
		sql.FlavorMySql:    "UPDATE mytable SET field1=?,field2=? WHERE (field1 > ?) AND (field2 = ?)",
		sql.FlavorSqlite:   "UPDATE mytable SET field1=?,field2=? WHERE (field1 > ?) AND (field2 = ?)",
		sql.FlavorDefault:  "UPDATE mytable SET field1=?,field2=? WHERE (field1 > ?) AND (field2 = ?)",
	}
	for _, flavor := range flavorList {
		filter := (&FilterAnd{}).Add(&FilterFieldValue{Field: "field1", Operator: ">", Value: 2}).Add(&FilterFieldValue{Field: "field2", Operator: "=", Value: "b"})
		// note: .WithFlavor also correctly sets PlaceholderGenerator
		builder := NewUpdateBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).WithFilter(filter).WithTable("mytable")
		builder.AddValues(map[string]interface{}{"1": 1, "2": "two"})
		if !reflect.DeepEqual(builder.Values, map[string]interface{}{"1": 1, "2": "two"}) {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, map[string]interface{}{"1": 1, "2": "two"}, builder.Values)
		}
		builder.WithValues(map[string]interface{}{"field1": 1, "field2": "a"})
		if !reflect.DeepEqual(builder.Values, map[string]interface{}{"field1": 1, "field2": "a"}) {
			t.Fatalf("%s failed: expected %#v but received %#v", testName, map[string]interface{}{"field1": 1, "field2": "a"}, builder.Values)
		}
		// note: .Build uses its internal DbFlavor, not the one supplied via parameters
		if sql, values := builder.Build(OptDbFlavor{sql.FlavorDefault}); sql != expectedSql[flavor] || len(values) != 4 || values[0] != 1 || values[1] != "a" || values[2] != 2 || values[3] != "b" {
			t.Fatalf("%s failed: (%#v)\nexpected: %#v\nreceived: %#v / %#v", testName, flavor, expectedSql[flavor], sql, values)
		}
	}
}

func TestUpdateBuilder_WithFilter_OptTableAlias(t *testing.T) {
	testName := "TestUpdateBuilder_WithFilter_OptTableAlias"
	flavorList := []sql.DbFlavor{sql.FlavorDefault, sql.FlavorMySql, sql.FlavorPgSql, sql.FlavorMsSql, sql.FlavorOracle, sql.FlavorSqlite, sql.FlavorCosmosDb}
	expectedSql := map[sql.DbFlavor]string{
		sql.FlavorPgSql:    "UPDATE mytable t SET t.field1=$1,t.field2=$2 WHERE (t.field1 > $3) OR (t.field2 = $4)",
		sql.FlavorCosmosDb: "UPDATE mytable SET field1=$1,field2=$2 WHERE (field1 > $3) OR (field2 = $4)",
		sql.FlavorMsSql:    "UPDATE mytable t SET t.field1=@p1,t.field2=@p2 WHERE (t.field1 > @p3) OR (t.field2 = @p4)",
		sql.FlavorOracle:   "UPDATE mytable t SET t.field1=:1,t.field2=:2 WHERE (t.field1 > :3) OR (t.field2 = :4)",
		sql.FlavorMySql:    "UPDATE mytable t SET t.field1=?,t.field2=? WHERE (t.field1 > ?) OR (t.field2 = ?)",
		sql.FlavorSqlite:   "UPDATE mytable t SET t.field1=?,t.field2=? WHERE (t.field1 > ?) OR (t.field2 = ?)",
		sql.FlavorDefault:  "UPDATE mytable t SET t.field1=?,t.field2=? WHERE (t.field1 > ?) OR (t.field2 = ?)",
	}
	for _, flavor := range flavorList {
		filter := (&FilterOr{}).Add(&FilterFieldValue{Field: "field1", Operator: ">", Value: 2}).Add(&FilterFieldValue{Field: "field2", Operator: "=", Value: "b"})
		// note: .WithFlavor also correctly sets PlaceholderGenerator
		builder := NewUpdateBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).WithFilter(filter).WithTable("mytable")
		builder.WithValues(map[string]interface{}{"field1": 1, "field2": "a"})
		if sql, values := builder.Build(&OptTableAlias{"t"}); sql != expectedSql[flavor] || len(values) != 4 || values[0] != 1 || values[1] != "a" || values[2] != 2 || values[3] != "b" {
			t.Fatalf("%s failed: (%#v)\nexpected: %#v\nreceived: %#v / %#v", testName, flavor, expectedSql[flavor], sql, values)
		}
	}
}

func TestUpdateBuilder_WithFilter_OptTableAliasFiltered(t *testing.T) {
	testName := "TestUpdateBuilder_WithFilter_OptTableAliasFiltered"
	flavorList := []sql.DbFlavor{sql.FlavorDefault, sql.FlavorMySql, sql.FlavorPgSql, sql.FlavorMsSql, sql.FlavorOracle, sql.FlavorSqlite, sql.FlavorCosmosDb}
	expectedSql := map[sql.DbFlavor]string{
		sql.FlavorPgSql:    "UPDATE mytable t0 SET t.field1=$1,t1.field2=$2 WHERE (t.field1 > $3) OR (t2.field2 = $4)",
		sql.FlavorCosmosDb: "UPDATE mytable t0 SET field1=$1,t1.field2=$2 WHERE (field1 > $3) OR (t2.field2 = $4)",
		sql.FlavorMsSql:    "UPDATE mytable t0 SET t.field1=@p1,t1.field2=@p2 WHERE (t.field1 > @p3) OR (t2.field2 = @p4)",
		sql.FlavorOracle:   "UPDATE mytable t0 SET t.field1=:1,t1.field2=:2 WHERE (t.field1 > :3) OR (t2.field2 = :4)",
		sql.FlavorMySql:    "UPDATE mytable t0 SET t.field1=?,t1.field2=? WHERE (t.field1 > ?) OR (t2.field2 = ?)",
		sql.FlavorSqlite:   "UPDATE mytable t0 SET t.field1=?,t1.field2=? WHERE (t.field1 > ?) OR (t2.field2 = ?)",
		sql.FlavorDefault:  "UPDATE mytable t0 SET t.field1=?,t1.field2=? WHERE (t.field1 > ?) OR (t2.field2 = ?)",
	}
	for _, flavor := range flavorList {
		filter := (&FilterOr{}).Add(&FilterFieldValue{Field: "field1", Operator: ">", Value: 2}).Add(&FilterFieldValue{Field: "t2.field2", Operator: "=", Value: "b"})
		// note: .WithFlavor also correctly sets PlaceholderGenerator
		builder := NewUpdateBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).WithFilter(filter).WithTable("mytable t0")
		builder.WithValues(map[string]interface{}{"field1": 1, "t1.field2": "a"})
		if sql, values := builder.Build(OptTableAlias{"t"}); sql != expectedSql[flavor] || len(values) != 4 || values[0] != 1 || values[1] != "a" || values[2] != 2 || values[3] != "b" {
			t.Fatalf("%s failed: (%#v)\nexpected: %#v\nreceived: %#v / %#v", testName, flavor, expectedSql[flavor], sql, values)
		}
	}
}

func TestSelectBuilder(t *testing.T) {
	testName := "TestSelectBuilder"
	flavorList := []sql.DbFlavor{sql.FlavorDefault, sql.FlavorMySql, sql.FlavorPgSql, sql.FlavorMsSql, sql.FlavorOracle, sql.FlavorSqlite, sql.FlavorCosmosDb}
	expectedSql := map[sql.DbFlavor]string{
		sql.FlavorPgSql:    "SELECT a,b,c FROM mytable WHERE (field1 > $1) AND (field2 = $2) GROUP BY cola,colb HAVING (cola > $3) OR (colb = colc) ORDER BY a,b DESC,c desc LIMIT 3 OFFSET 5",
		sql.FlavorCosmosDb: "SELECT c.a,c.b,c.c FROM mytable c WHERE (c.field1 > $1) AND (c.field2 = $2) GROUP BY c.cola,c.colb HAVING (c.cola > $3) OR (c.colb = c.colc) ORDER BY c.a,c.b DESC,c.c desc OFFSET 5 LIMIT 3 WITH cross_partition=true",
		sql.FlavorMsSql:    "SELECT a,b,c FROM mytable WHERE (field1 > @p1) AND (field2 = @p2) GROUP BY cola,colb HAVING (cola > @p3) OR (colb = colc) ORDER BY a,b DESC,c desc OFFSET 5 ROWS FETCH NEXT 3 ROWS ONLY",
		sql.FlavorOracle:   "SELECT a,b,c FROM mytable WHERE (field1 > :1) AND (field2 = :2) GROUP BY cola,colb HAVING (cola > :3) OR (colb = colc) ORDER BY a,b DESC,c desc OFFSET 5 ROWS FETCH NEXT 3 ROWS ONLY",
		sql.FlavorMySql:    "SELECT a,b,c FROM mytable WHERE (field1 > ?) AND (field2 = ?) GROUP BY cola,colb HAVING (cola > ?) OR (colb = colc) ORDER BY a,b DESC,c desc LIMIT 5,3",
		sql.FlavorSqlite:   "SELECT a,b,c FROM mytable WHERE (field1 > ?) AND (field2 = ?) GROUP BY cola,colb HAVING (cola > ?) OR (colb = colc) ORDER BY a,b DESC,c desc LIMIT 5,3",
		sql.FlavorDefault:  "SELECT a,b,c FROM mytable WHERE (field1 > ?) AND (field2 = ?) GROUP BY cola,colb HAVING (cola > ?) OR (colb = colc) ORDER BY a,b DESC,c desc",
	}
	for _, flavor := range flavorList {
		filterMain := (&FilterAnd{}).Add(&FilterFieldValue{Field: "field1", Operator: ">", Value: 1}).
			Add(&FilterFieldValue{Field: "field2", Operator: "=", Value: "a"})
		// note: .WithFlavor also correctly sets PlaceholderGenerator
		builder := NewSelectBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).WithFilter(filterMain)

		builder.WithTable("table0").AddTables("table1", "table2")
		expectedTables := []string{"table0", "table1", "table2"}
		if !reflect.DeepEqual(builder.Tables, expectedTables) {
			t.Fatalf("%s failed: expected %#v / received %#v", testName, expectedTables, builder.Tables)
		}
		builder.WithTables("table3", "table4").AddTables("table5")
		expectedTables = []string{"table3", "table4", "table5"}
		if !reflect.DeepEqual(builder.Tables, expectedTables) {
			t.Fatalf("%s failed: expected %#v / received %#v", testName, expectedTables, builder.Tables)
		}

		builder.WithColumns("a", "b", "c")
		builder.AddColumns("col1", "col2", "col3")
		expectedCols := []string{"a", "b", "c", "col1", "col2", "col3"}
		if !reflect.DeepEqual(builder.Columns, expectedCols) {
			t.Fatalf("%s failed: expected %#v / received %#v", testName, expectedCols, builder.Columns)
		}

		builder.WithGroupBy("a", "b")
		builder.AddGroupBy("cola", "colb", "colc")
		expectedGroupBy := []string{"a", "b", "cola", "colb", "colc"}
		if !reflect.DeepEqual(builder.GroupBy, expectedGroupBy) {
			t.Fatalf("%s failed: expected %#v / received %#v", testName, expectedGroupBy, builder.GroupBy)
		}

		builder.WithTables("mytable")
		builder.WithColumns("a", "b", "c")
		builder.WithGroupBy("cola", "colb")
		filterHaving := (&FilterOr{}).Add(&FilterFieldValue{Field: "cola", Operator: ">", Value: 2}).
			Add(&FilterExpression{Left: "colb", Operator: "=", Right: "colc"})
		builder.WithHaving(filterHaving)
		builder.WithSorting((&GenericSorting{}).Add("a").Add("b:-1").Add("c:desc"))
		builder.WithLimit(3, 5)
		// note: .Build uses its internal DbFlavor, not the one supplied via parameters
		if sql, values := builder.Build(OptDbFlavor{sql.FlavorDefault}); sql != expectedSql[flavor] || len(values) != 3 || values[0] != 1 || values[1] != "a" || values[2] != 2 {
			t.Fatalf("%s failed: (%#v) expected\n%#v\nbut received\n%#v / %#v", testName+"/"+strconv.Itoa(int(flavor)), flavor, expectedSql[flavor], sql, values)
		}
	}
}

func TestSelectBuilder_OptTableAlias(t *testing.T) {
	testName := "TestSelectBuilder_OptTableAlias"
	flavorList := []sql.DbFlavor{sql.FlavorDefault, sql.FlavorMySql, sql.FlavorPgSql, sql.FlavorMsSql, sql.FlavorOracle, sql.FlavorSqlite, sql.FlavorCosmosDb}
	expectedSql := map[sql.DbFlavor]string{
		sql.FlavorPgSql:    "SELECT t.a,t.b,t.c FROM mytable t WHERE (t.field1 > $1) AND (t.field2 = $2) GROUP BY t.cola,t.colb HAVING (t.cola > $3) OR (t.colb = t.colc) ORDER BY t.a,t.b DESC,t.c desc LIMIT 3 OFFSET 5",
		sql.FlavorCosmosDb: "SELECT t.a,t.b,t.c FROM mytable t WHERE (t.field1 > $1) AND (t.field2 = $2) GROUP BY t.cola,t.colb HAVING (t.cola > $3) OR (t.colb = t.colc) ORDER BY t.a,t.b DESC,t.c desc OFFSET 5 LIMIT 3 WITH cross_partition=true",
		sql.FlavorMsSql:    "SELECT t.a,t.b,t.c FROM mytable t WHERE (t.field1 > @p1) AND (t.field2 = @p2) GROUP BY t.cola,t.colb HAVING (t.cola > @p3) OR (t.colb = t.colc) ORDER BY t.a,t.b DESC,t.c desc OFFSET 5 ROWS FETCH NEXT 3 ROWS ONLY",
		sql.FlavorOracle:   "SELECT t.a,t.b,t.c FROM mytable t WHERE (t.field1 > :1) AND (t.field2 = :2) GROUP BY t.cola,t.colb HAVING (t.cola > :3) OR (t.colb = t.colc) ORDER BY t.a,t.b DESC,t.c desc OFFSET 5 ROWS FETCH NEXT 3 ROWS ONLY",
		sql.FlavorMySql:    "SELECT t.a,t.b,t.c FROM mytable t WHERE (t.field1 > ?) AND (t.field2 = ?) GROUP BY t.cola,t.colb HAVING (t.cola > ?) OR (t.colb = t.colc) ORDER BY t.a,t.b DESC,t.c desc LIMIT 5,3",
		sql.FlavorSqlite:   "SELECT t.a,t.b,t.c FROM mytable t WHERE (t.field1 > ?) AND (t.field2 = ?) GROUP BY t.cola,t.colb HAVING (t.cola > ?) OR (t.colb = t.colc) ORDER BY t.a,t.b DESC,t.c desc LIMIT 5,3",
		sql.FlavorDefault:  "SELECT t.a,t.b,t.c FROM mytable t WHERE (t.field1 > ?) AND (t.field2 = ?) GROUP BY t.cola,t.colb HAVING (t.cola > ?) OR (t.colb = t.colc) ORDER BY t.a,t.b DESC,t.c desc",
	}
	for _, flavor := range flavorList {
		filterMain := (&FilterAnd{}).Add(&FilterFieldValue{Field: "field1", Operator: ">", Value: 1}).
			Add(&FilterFieldValue{Field: "field2", Operator: "=", Value: "a"})
		// note: .WithFlavor also correctly sets PlaceholderGenerator
		builder := NewSelectBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).WithFilter(filterMain)
		builder.WithTables("mytable")
		builder.WithColumns("a", "b", "c")
		builder.WithGroupBy("cola", "colb")
		filterHaving := (&FilterOr{}).Add(&FilterFieldValue{Field: "cola", Operator: ">", Value: 2}).
			Add(&FilterExpression{Left: "colb", Operator: "=", Right: "colc"})
		builder.WithHaving(filterHaving)
		builder.WithSorting((&GenericSorting{}).Add("a").Add("b:-1").Add("c:desc"))
		builder.WithLimit(3, 5)
		// note: .Build uses its internal DbFlavor, not the one supplied via parameters
		if sql, values := builder.Build(&OptDbFlavor{sql.FlavorDefault}, OptTableAlias{"t"}); sql != expectedSql[flavor] || len(values) != 3 || values[0] != 1 || values[1] != "a" || values[2] != 2 {
			t.Fatalf("%s failed: (%#v) expected\n%#v\nbut received\n%#v / %#v", testName+"/"+strconv.Itoa(int(flavor)), flavor, expectedSql[flavor], sql, values)
		}
	}
}

func TestSelectBuilder_OptTableAliasFiltered(t *testing.T) {
	testName := "TestSelectBuilder_OptTableAliasFiltered"
	flavorList := []sql.DbFlavor{sql.FlavorDefault, sql.FlavorMySql, sql.FlavorPgSql, sql.FlavorMsSql, sql.FlavorOracle, sql.FlavorSqlite, sql.FlavorCosmosDb}
	expectedSql := map[sql.DbFlavor]string{
		sql.FlavorPgSql:    "SELECT t.a,t1.b,t.c FROM mytable t0 WHERE (t.field1 > $1) AND (t2.field2 = $2) GROUP BY t.cola,t3.colb HAVING (t.cola > $3) OR (t4.colb = t.colc) ORDER BY t.a,t5.b DESC,t.c desc LIMIT 3 OFFSET 5",
		sql.FlavorCosmosDb: "SELECT t.a,t1.b,t.c FROM mytable t0 WHERE (t.field1 > $1) AND (t2.field2 = $2) GROUP BY t.cola,t3.colb HAVING (t.cola > $3) OR (t4.colb = t.colc) ORDER BY t.a,t5.b DESC,t.c desc OFFSET 5 LIMIT 3 WITH cross_partition=true",
		sql.FlavorMsSql:    "SELECT t.a,t1.b,t.c FROM mytable t0 WHERE (t.field1 > @p1) AND (t2.field2 = @p2) GROUP BY t.cola,t3.colb HAVING (t.cola > @p3) OR (t4.colb = t.colc) ORDER BY t.a,t5.b DESC,t.c desc OFFSET 5 ROWS FETCH NEXT 3 ROWS ONLY",
		sql.FlavorOracle:   "SELECT t.a,t1.b,t.c FROM mytable t0 WHERE (t.field1 > :1) AND (t2.field2 = :2) GROUP BY t.cola,t3.colb HAVING (t.cola > :3) OR (t4.colb = t.colc) ORDER BY t.a,t5.b DESC,t.c desc OFFSET 5 ROWS FETCH NEXT 3 ROWS ONLY",
		sql.FlavorMySql:    "SELECT t.a,t1.b,t.c FROM mytable t0 WHERE (t.field1 > ?) AND (t2.field2 = ?) GROUP BY t.cola,t3.colb HAVING (t.cola > ?) OR (t4.colb = t.colc) ORDER BY t.a,t5.b DESC,t.c desc LIMIT 5,3",
		sql.FlavorSqlite:   "SELECT t.a,t1.b,t.c FROM mytable t0 WHERE (t.field1 > ?) AND (t2.field2 = ?) GROUP BY t.cola,t3.colb HAVING (t.cola > ?) OR (t4.colb = t.colc) ORDER BY t.a,t5.b DESC,t.c desc LIMIT 5,3",
		sql.FlavorDefault:  "SELECT t.a,t1.b,t.c FROM mytable t0 WHERE (t.field1 > ?) AND (t2.field2 = ?) GROUP BY t.cola,t3.colb HAVING (t.cola > ?) OR (t4.colb = t.colc) ORDER BY t.a,t5.b DESC,t.c desc",
	}
	for _, flavor := range flavorList {
		filterMain := (&FilterAnd{}).Add(&FilterFieldValue{Field: "field1", Operator: ">", Value: 1}).
			Add(&FilterFieldValue{Field: "t2.field2", Operator: "=", Value: "a"})
		// note: .WithFlavor also correctly sets PlaceholderGenerator
		builder := NewSelectBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).WithFilter(filterMain)
		builder.WithTables("mytable t0")
		builder.WithColumns("a", "t1.b", "c")
		builder.WithGroupBy("cola", "t3.colb")
		filterHaving := (&FilterOr{}).Add(&FilterFieldValue{Field: "cola", Operator: ">", Value: 2}).
			Add(&FilterExpression{Left: "t4.colb", Operator: "=", Right: "colc"})
		builder.WithHaving(filterHaving)
		builder.WithSorting((&GenericSorting{}).Add("a").Add("t5.b:-1").Add("c:desc"))
		builder.WithLimit(3, 5)
		// note: .Build uses its internal DbFlavor, not the one supplied via parameters
		if sql, values := builder.Build(OptDbFlavor{sql.FlavorDefault}, &OptTableAlias{"t"}); sql != expectedSql[flavor] || len(values) != 3 || values[0] != 1 || values[1] != "a" || values[2] != 2 {
			t.Fatalf("%s failed: (%#v) expected\n%#v\nbut received\n%#v / %#v", testName+"/"+strconv.Itoa(int(flavor)), flavor, expectedSql[flavor], sql, values)
		}
	}
}
