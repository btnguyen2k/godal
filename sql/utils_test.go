package sql

import (
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/btnguyen2k/prom/sql"
)

func TestNewPlaceholderGeneratorQuestion(t *testing.T) {
	name := "TestNewPlaceholderGeneratorQuestion"
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
			t.Fatalf("%s failed: expected %#v at index %d but received %#v", name, expected, i, result)
		}
	}
}

func TestNewPlaceholderGeneratorDollarN(t *testing.T) {
	name := "TestNewPlaceholderGeneratorDollarN"
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
			t.Fatalf("%s failed: expected %#v at index %d but received %#v", name, expected, i, result)
		}
	}
}

func TestNewPlaceholderGeneratorColonN(t *testing.T) {
	name := "TestNewPlaceholderGeneratorColonN"
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
			t.Fatalf("%s failed: expected %#v at index %d but received %#v", name, expected, i, result)
		}
	}
}

func TestNewPlaceholderGeneratorAtpiN(t *testing.T) {
	name := "TestNewPlaceholderGeneratorAtpiN"
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
			t.Fatalf("%s failed: expected %#v at index %d but received %#v", name, expected, i, result)
		}
	}
}

func Test_GenericSorting(t *testing.T) {
	name := "Test_GenericSorting"

	sorting := &GenericSorting{}
	if clause := sorting.Build(); clause != "" {
		t.Fatalf("%s failed: expected empty", name)
	}

	sorting = (&GenericSorting{}).Add("field1:1").Add("field2:-1").Add("field3:asc").Add("field4:desc").Add("field5")
	if clause := sorting.Build(); clause != "field1,field2 DESC,field3 asc,field4 desc,field5" {
		t.Fatalf("%s failed: %s", name, clause)
	}

	sorting = (&GenericSorting{}).Add("field1:1").Add("field2:-1").Add("field3:asc").Add("field4:desc").Add("field5")
	if clause := sorting.Build(OptTableAlias{TableAlias: "t"}); clause != "t.field1,t.field2 DESC,t.field3 asc,t.field4 desc,t.field5" {
		t.Fatalf("%s failed: %s", name, clause)
	}

	sorting = (&GenericSorting{}).Add("field1:1").Add("field2:-1").Add("field3:asc").Add("field4:desc").Add("field5")
	if clause := sorting.Build(&OptTableAlias{TableAlias: "c"}); clause != "c.field1,c.field2 DESC,c.field3 asc,c.field4 desc,c.field5" {
		t.Fatalf("%s failed: %s", name, clause)
	}
}

func TestFilterAnd(t *testing.T) {
	name := "TestFilterAnd"
	filter := &FilterAnd{}

	if clause, values := filter.Build(nil); clause != "" || len(values) != 0 {
		t.Fatalf("%s failed: expected empty/0 but received %#v/%#v", name, clause, len(values))
	}

	placeholderGenerator := NewPlaceholderGeneratorQuestion()
	filter.Add(&FilterFieldValue{Field: "field1", Operator: ">", Value: 1}).
		Add(&FilterFieldValue{Field: "field2", Operator: "=", Value: "a"})
	if clause, values := filter.Build(placeholderGenerator); clause != "(field1 > ?) AND (field2 = ?)" || len(values) != 2 || values[0] != 1 || values[1] != "a" {
		t.Fatalf("%s failed: %#v/%#v", name, clause, values)
	}

	if clause, values := filter.Build(placeholderGenerator, OptTableAlias{TableAlias: "t"}); clause != "(t.field1 > ?) AND (t.field2 = ?)" || len(values) != 2 || values[0] != 1 || values[1] != "a" {
		t.Fatalf("%s failed: %#v/%#v", name, clause, values)
	}

	if clause, values := filter.Build(placeholderGenerator, &OptTableAlias{TableAlias: "c"}); clause != "(c.field1 > ?) AND (c.field2 = ?)" || len(values) != 2 || values[0] != 1 || values[1] != "a" {
		t.Fatalf("%s failed: %#v/%#v", name, clause, values)
	}
}

func TestFilterOr(t *testing.T) {
	name := "TestFilterOr"
	filter := &FilterOr{}

	if clause, values := filter.Build(nil); clause != "" || len(values) != 0 {
		t.Fatalf("%s failed: expected empty/0 but received %#v/%#v", name, clause, len(values))
	}

	placeholderGenerator := NewPlaceholderGeneratorQuestion()
	filter.Add(&FilterExpression{Left: "field1", Operator: ">", Right: "field2"}).
		Add(&FilterExpression{Left: "field3", Operator: "IS", Right: "null"})
	if clause, values := filter.Build(placeholderGenerator); clause != "(field1 > field2) OR (field3 IS null)" || len(values) != 0 {
		t.Fatalf("%s failed: %#v/%#v", name, clause, values)
	}
}

func TestFilterIsNull(t *testing.T) {
	name := "TestFilterIsNull"
	filter := &FilterIsNull{}

	filter.Field = "myfield"
	if clause, values := filter.Build(nil); clause != "myfield IS NULL" || len(values) != 0 {
		t.Fatalf("%s failed: %#v/%#v", name, clause, values)
	}

	opt := OptTableAlias{TableAlias: "myalias"}
	if clause, values := filter.Build(nil, opt); clause != "myalias.myfield IS NULL" || len(values) != 0 {
		t.Fatalf("%s failed: %#v/%#v", name, clause, values)
	}
	if clause, values := filter.Build(nil, &opt); clause != "myalias.myfield IS NULL" || len(values) != 0 {
		t.Fatalf("%s failed: %#v/%#v", name, clause, values)
	}
}

func TestFilterIsNotNull(t *testing.T) {
	name := "TestFilterIsNotNull"
	filter := &FilterIsNotNull{}

	filter.Field = "myfield"
	if clause, values := filter.Build(nil); clause != "myfield IS NOT NULL" || len(values) != 0 {
		t.Fatalf("%s failed: %#v/%#v", name, clause, values)
	}

	opt := OptTableAlias{TableAlias: "myalias"}
	if clause, values := filter.Build(nil, opt); clause != "myalias.myfield IS NOT NULL" || len(values) != 0 {
		t.Fatalf("%s failed: %#v/%#v", name, clause, values)
	}
	if clause, values := filter.Build(nil, &opt); clause != "myalias.myfield IS NOT NULL" || len(values) != 0 {
		t.Fatalf("%s failed: %#v/%#v", name, clause, values)
	}
}

func TestFilterBetween(t *testing.T) {
	name := "TestFilterBetween"
	filter := &FilterBetween{Field: "myfield", ValueLeft: 0, ValueRight: 9}

	if clause, values := filter.Build(nil); clause != "" || len(values) != 0 {
		t.Fatalf("%s failed: expected empty/0 but received %#v/%#v", name, clause, len(values))
	}

	placeholderGenerator := NewPlaceholderGeneratorQuestion()
	if clause, values := filter.Build(placeholderGenerator); clause != "myfield BETWEEN ? AND ?" || len(values) != 2 {
		t.Fatalf("%s failed: %#v/%#v", name, clause, values)
	}

	opt := OptTableAlias{TableAlias: "myalias"}
	if clause, values := filter.Build(placeholderGenerator, opt); clause != "myalias.myfield BETWEEN ? AND ?" || len(values) != 2 {
		t.Fatalf("%s failed: %#v/%#v", name, clause, values)
	}
	if clause, values := filter.Build(placeholderGenerator, &opt); clause != "myalias.myfield BETWEEN ? AND ?" || len(values) != 2 {
		t.Fatalf("%s failed: %#v/%#v", name, clause, values)
	}
}

func Test_DeleteBuilder(t *testing.T) {
	name := "Test_DeleteBuilder"
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
		builder := NewDeleteBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).WithTable("mytable")

		if sql, values := builder.Build(); sql != "DELETE FROM mytable" || len(values) != 0 {
			t.Fatalf("%s failed: %#v / %#v", name, sql, values)
		}

		filter := (&FilterAnd{}).Add(&FilterFieldValue{Field: "field1", Operator: ">", Value: 1}).Add(&FilterFieldValue{Field: "field2", Operator: "=", Value: "a"})
		builder.WithFilter(filter)
		if sql, values := builder.Build(); sql != expectedSql[flavor] || len(values) != 2 || values[0] != 1 || values[1] != "a" {
			t.Fatalf("%s failed: (%#v) expected %#v but received %#v / %#v", name, flavor, expectedSql[flavor], sql, values)
		}
	}
}

func Test_InsertBuilder(t *testing.T) {
	name := "Test_InsertBuilder"
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
		builder := NewInsertBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).
			WithTable("mytable").AddValues(map[string]interface{}{"field1": 1, "field2": "a"}).
			WithValues(map[string]interface{}{"field1": 1, "field2": "a"})
		if sql, values := builder.Build(); sql != expectedSql[flavor] || len(values) != 2 || values[0] != 1 || values[1] != "a" {
			t.Fatalf("%s failed: (%#v) expected %#v but received %#v / %#v", name, flavor, expectedSql[flavor], sql, values)
		}
	}
}

func Test_UpdateBuilder_NoFilter(t *testing.T) {
	name := "Test_UpdateBuilder_NoFilter"
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
		builder := NewUpdateBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).WithTable("mytable")
		builder.AddValues(map[string]interface{}{"1": 1, "2": "two"})
		if !reflect.DeepEqual(builder.Values, map[string]interface{}{"1": 1, "2": "two"}) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, map[string]interface{}{"1": 1, "2": "two"}, builder.Values)
		}
		builder.WithValues(map[string]interface{}{"field1": 1, "field2": "a"})
		if !reflect.DeepEqual(builder.Values, map[string]interface{}{"field1": 1, "field2": "a"}) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, map[string]interface{}{"field1": 1, "field2": "a"}, builder.Values)
		}
		if sql, values := builder.Build(); sql != expectedSql[flavor] || len(values) != 2 || values[0] != 1 || values[1] != "a" {
			t.Fatalf("%s failed: (%#v) expected %#v but received %#v / %#v", name, flavor, expectedSql[flavor], sql, values)
		}
	}
}

func Test_UpdateBuilder_WithFilter(t *testing.T) {
	name := "Test_UpdateBuilder_NoWhere"
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
		builder := NewUpdateBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).WithFilter(filter).WithTable("mytable")
		builder.AddValues(map[string]interface{}{"1": 1, "2": "two"})
		if !reflect.DeepEqual(builder.Values, map[string]interface{}{"1": 1, "2": "two"}) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, map[string]interface{}{"1": 1, "2": "two"}, builder.Values)
		}
		builder.WithValues(map[string]interface{}{"field1": 1, "field2": "a"})
		if !reflect.DeepEqual(builder.Values, map[string]interface{}{"field1": 1, "field2": "a"}) {
			t.Fatalf("%s failed: expected %#v but received %#v", name, map[string]interface{}{"field1": 1, "field2": "a"}, builder.Values)
		}
		if sql, values := builder.Build(); sql != expectedSql[flavor] || len(values) != 4 || values[0] != 1 || values[1] != "a" || values[2] != 2 || values[3] != "b" {
			t.Fatalf("%s failed: (%#v) expected %#v but received %#v / %#v", name, flavor, expectedSql[flavor], sql, values)
		}
	}
}

func Test_SelectBuilder(t *testing.T) {
	name := "Test_SelectBuilder"
	flavorList := []sql.DbFlavor{sql.FlavorDefault, sql.FlavorMySql, sql.FlavorPgSql, sql.FlavorMsSql, sql.FlavorOracle, sql.FlavorSqlite, sql.FlavorCosmosDb}
	expectedSql := map[sql.DbFlavor]string{
		sql.FlavorPgSql:    "SELECT a,b,c FROM mytable WHERE (field1 > $1) AND (field2 = $2) GROUP BY cola,colb HAVING (cola > $3) OR (colb = b) ORDER BY a,b DESC,c desc LIMIT 3 OFFSET 5",
		sql.FlavorCosmosDb: "SELECT c.a,c.b,c.c FROM mytable c WHERE (c.field1 > $1) AND (c.field2 = $2) GROUP BY c.cola,c.colb HAVING (c.cola > $3) OR (colb = b) ORDER BY c.a,c.b DESC,c.c desc OFFSET 5 LIMIT 3 WITH cross_partition=true",
		sql.FlavorMsSql:    "SELECT a,b,c FROM mytable WHERE (field1 > @p1) AND (field2 = @p2) GROUP BY cola,colb HAVING (cola > @p3) OR (colb = b) ORDER BY a,b DESC,c desc OFFSET 5 ROWS FETCH NEXT 3 ROWS ONLY",
		sql.FlavorOracle:   "SELECT a,b,c FROM mytable WHERE (field1 > :1) AND (field2 = :2) GROUP BY cola,colb HAVING (cola > :3) OR (colb = b) ORDER BY a,b DESC,c desc OFFSET 5 ROWS FETCH NEXT 3 ROWS ONLY",
		sql.FlavorMySql:    "SELECT a,b,c FROM mytable WHERE (field1 > ?) AND (field2 = ?) GROUP BY cola,colb HAVING (cola > ?) OR (colb = b) ORDER BY a,b DESC,c desc LIMIT 5,3",
		sql.FlavorSqlite:   "SELECT a,b,c FROM mytable WHERE (field1 > ?) AND (field2 = ?) GROUP BY cola,colb HAVING (cola > ?) OR (colb = b) ORDER BY a,b DESC,c desc LIMIT 5,3",
		sql.FlavorDefault:  "SELECT a,b,c FROM mytable WHERE (field1 > ?) AND (field2 = ?) GROUP BY cola,colb HAVING (cola > ?) OR (colb = b) ORDER BY a,b DESC,c desc",
	}
	for _, flavor := range flavorList {
		filterMain := (&FilterAnd{}).Add(&FilterFieldValue{Field: "field1", Operator: ">", Value: 1}).Add(&FilterFieldValue{Field: "field2", Operator: "=", Value: "a"})
		builder := NewSelectBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).WithFilter(filterMain)

		builder.WithTable("table0").AddTables("table1", "table2")
		if builder.Table != "table0" || reflect.DeepEqual(builder.Tables, []string{"table1", "table2"}) {
			t.Fatalf("%s failed: %#v / %#v", name, builder.Table, builder.Tables)
		}
		builder.WithTables("mytable")

		builder.AddColumns("col1", "col2", "col3")
		if !reflect.DeepEqual(builder.Columns, []string{"col1", "col2", "col3"}) {
			t.Fatalf("%s failed: %#v", name, builder.Columns)
		}
		builder.WithColumns("a", "b", "c")

		builder.AddGroupBy("cola", "colb", "colc")
		if !reflect.DeepEqual(builder.GroupBy, []string{"cola", "colb", "colc"}) {
			t.Fatalf("%s failed: %#v", name, builder.GroupBy)
		}
		builder.WithGroupBy("cola", "colb")

		filterHaving := (&FilterOr{}).Add(&FilterFieldValue{Field: "cola", Operator: ">", Value: 2}).Add(&FilterExpression{Left: "colb", Operator: "=", Right: "b"})
		builder.WithHaving(filterHaving)

		builder.WithSorting((&GenericSorting{}).Add("a").Add("b:-1").Add("c:desc"))
		builder.WithLimit(3, 5)

		if sql, values := builder.Build(); sql != expectedSql[flavor] || len(values) != 3 || values[0] != 1 || values[1] != "a" || values[2] != 2 {
			t.Fatalf("%s failed: (%#v) expected\n%#v\nbut received\n%#v / %#v", name, flavor, expectedSql[flavor], sql, values)
		}
	}
}

func Test_SelectBuilderCosmosdb(t *testing.T) {
	name := "Test_SelectBuilderCosmosdb"
	flavor := sql.FlavorCosmosDb
	expectedSql := "SELECT m.a,m.b,m.c FROM mytable m WHERE (m.field1 > $1) AND (m.field2 = $2) GROUP BY m.cola,m.colb HAVING (m.cola > $3) OR (colb = b) ORDER BY m.a,m.b DESC,m.c desc OFFSET 5 LIMIT 3 WITH cross_partition=true"
	filterMain := (&FilterAnd{}).Add(&FilterFieldValue{Field: "field1", Operator: ">", Value: 1}).Add(&FilterFieldValue{Field: "field2", Operator: "=", Value: "a"})
	builder := NewSelectBuilder().WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion()).WithFlavor(flavor).
		WithFilter(filterMain).WithTables("mytable m").WithColumns("a", "b", "c").
		WithGroupBy("cola", "colb")
	filterHaving := (&FilterOr{}).Add(&FilterFieldValue{Field: "cola", Operator: ">", Value: 2}).Add(&FilterExpression{Left: "colb", Operator: "=", Right: "b"})
	builder.WithHaving(filterHaving)
	builder.WithSorting((&GenericSorting{}).Add("a").Add("b:-1").Add("c:desc"))
	builder.WithLimit(3, 5)

	if sql, values := builder.Build(); sql != expectedSql || len(values) != 3 || values[0] != 1 || values[1] != "a" || values[2] != 2 {
		t.Fatalf("%s failed: (%#v) expected\n%#v\nbut received\n%#v / %#v", name, flavor, expectedSql, sql, values)
	}

	expectedSql = "SELECT t.a,t.b,t.c FROM mytable t WHERE (t.field1 > $1) AND (t.field2 = $2) GROUP BY t.cola,t.colb HAVING (t.cola > $3) OR (colb = b) ORDER BY t.a,t.b DESC,t.c desc OFFSET 5 LIMIT 3 WITH cross_partition=true"
	builder.WithTables("mytable").WithFlavor(flavor)
	if sql, values := builder.Build(&OptTableAlias{TableAlias: "t"}); sql != expectedSql || len(values) != 3 || values[0] != 1 || values[1] != "a" || values[2] != 2 {
		t.Fatalf("%s failed: (%#v) expected\n%#v\nbut received\n%#v / %#v", name, flavor, expectedSql, sql, values)
	}

	expectedSql = "SELECT k.a,k.b,k.c FROM mytable k WHERE (k.field1 > $1) AND (k.field2 = $2) GROUP BY k.cola,k.colb HAVING (k.cola > $3) OR (colb = b) ORDER BY k.a,k.b DESC,k.c desc OFFSET 5 LIMIT 3 WITH cross_partition=true"
	builder.WithTables("mytable").WithFlavor(flavor)
	if sql, values := builder.Build(OptTableAlias{TableAlias: "k"}); sql != expectedSql || len(values) != 3 || values[0] != 1 || values[1] != "a" || values[2] != 2 {
		t.Fatalf("%s failed: (%#v) expected\n%#v\nbut received\n%#v / %#v", name, flavor, expectedSql, sql, values)
	}
}
