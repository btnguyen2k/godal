package sql

import (
	"encoding/json"
	"fmt"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/consu/semita"
	"github.com/btnguyen2k/godal"
	"strconv"
	"sync"
	"testing"
)

const timeZone = "Asia/Ho_Chi_Minh"

type MyBo struct {
	Id       string `json:"id"`
	Username string `json:"username"`
	Name     string `json:"name"`
	Version  int    `json:"version"`
}

func (bo *MyBo) ToGbo() godal.IGenericBo {
	js, _ := json.Marshal(bo)
	gbo := godal.NewGenericBo()
	if err := gbo.GboImportViaJson(map[string]interface{}{fieldGboId: bo.Id, fieldGboUsername: bo.Username, fieldGboData: string(js)}); err != nil {
		panic(err)
	}
	return gbo
}

func fromGbo(gbo godal.IGenericBo) *MyBo {
	js := gbo.GboGetAttrUnsafe(fieldGboData, reddo.TypeString).(string)
	bo := MyBo{}
	if err := json.Unmarshal([]byte(js), &bo); err != nil {
		panic(err)
	}
	return &bo
}

const (
	tableName   = "test"
	colId       = "id"
	colUsername = "username"
	colData     = "data"

	fieldGboId       = "id"
	fieldGboUsername = "username"
	fieldGboData     = "data"
)

type MyRowMapperSql struct {
}

func (mapper *MyRowMapperSql) ColumnsList(storageId string) []string {
	return []string{colId, colUsername, colData}
}

func (mapper *MyRowMapperSql) ToRow(storageId string, gbo godal.IGenericBo) (interface{}, error) {
	if gbo == nil {
		return nil, nil
	}
	return map[string]interface{}{
		colId:       gbo.GboGetAttrUnsafe(fieldGboId, reddo.TypeString),
		colUsername: gbo.GboGetAttrUnsafe(fieldGboUsername, reddo.TypeString),
		colData:     gbo.GboGetAttrUnsafe(fieldGboData, reddo.TypeString),
	}, nil
}

func (mapper *MyRowMapperSql) ToBo(storageId string, row interface{}) (godal.IGenericBo, error) {
	if row == nil {
		return nil, nil
	}
	s := semita.NewSemita(row)
	id, _ := s.GetValueOfType(colId, reddo.TypeString)
	username, _ := s.GetValueOfType(colUsername, reddo.TypeString)
	data, _ := s.GetValueOfType(colData, reddo.TypeString)
	bo := godal.NewGenericBo()
	bo.GboSetAttr(fieldGboId, id)
	bo.GboSetAttr(fieldGboUsername, username)
	bo.GboSetAttr(fieldGboData, data)
	return bo, nil
}

/*----------------------------------------------------------------------*/
func testGenericDao_Empty(dao godal.IGenericDao, tableName string, t *testing.T) {
	name := "TestGenericDao_Empty"
	boList, err := dao.GdaoFetchMany(tableName, nil, nil, 0, 0)
	if err != nil {
		t.Fatalf("%s failed, has error: %e", name, err)
	}
	if boList == nil {
		t.Fatalf("%s failed, nil result", name)
	}
	if len(boList) != 0 {
		t.Fatalf("%s failed, non-empty result: %v", name, boList)
	}

	bo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{colId: "any"})
	if err != nil {
		t.Fatalf("%s failed, has error: %e", name, err)
	}
	if bo != nil {
		t.Fatalf("%s failed, should have nill result, but received: %v", name, bo)
	}
}

func testGenericDao_GdaoCreateDuplicated(dao godal.IGenericDao, tableName string, t *testing.T) {
	name := "TestGenericDao_GdaoCreateDuplicated"
	bo1 := &MyBo{
		Id:       "1",
		Username: "1",
		Name:     "BO - 1",
		Version:  1,
	}
	if numRows, err := dao.GdaoCreate(tableName, bo1.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	bo2 := &MyBo{
		Id:       "2",
		Username: "1",
		Name:     "BO - 2",
		Version:  2,
	}
	if numRows, err := dao.GdaoCreate(tableName, bo2.ToGbo()); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
}

func testGenericDao_GdaoCreateGet(dao godal.IGenericDao, tableName string, t *testing.T) {
	name := "TestGenericDao_GdaoCreateGet"
	bo := &MyBo{
		Id:       "1",
		Username: "2",
		Name:     "BO - 3",
		Version:  4,
	}
	if numRows, err := dao.GdaoCreate(tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	gbo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{colId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Username != bo.Username || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}
}

func testGenericDao_GdaoCreateTwiceGet(dao godal.IGenericDao, tableName string, t *testing.T) {
	name := "TestGenericDao_GdaoCreateTwiceGet"
	bo := &MyBo{
		Id:       "1",
		Username: "2",
		Name:     "BO - 3",
		Version:  4,
	}
	if numRows, err := dao.GdaoCreate(tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	bo.Version = bo.Version + 1
	if numRows, err := dao.GdaoCreate(tableName, bo.ToGbo()); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	bo.Version = bo.Version - 1
	gbo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{colId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}
}

func testGenericDao_GdaoCreateMultiThreadsGet(dao godal.IGenericDao, tableName string, t *testing.T) {
	name := "TestGenericDao_GdaoCreateMultiThreadsGet"
	numThreads := 4
	numLoopsPerThread := 10
	var wg sync.WaitGroup
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(threadNum int, bo *MyBo) {
			defer wg.Done()
			for j := 0; j < numLoopsPerThread; j++ {
				if _, err := dao.GdaoCreate(tableName, bo.ToGbo()); err != nil && err != godal.GdaoErrorDuplicatedEntry {
					t.Fatalf("%s failed - Thread: %v / Error: %e", name, threadNum, err)
				}
				bo.Version = bo.Version + 1
			}
		}(i, &MyBo{
			Id:       "1",
			Username: "2",
			Name:     "BO - " + strconv.Itoa(i+1),
			Version:  3,
		})
	}
	wg.Wait()

	gbo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{colId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != "1" || myBo.Username != "2" || myBo.Version != 3 {
		t.Fatalf("%s failed - Received: %v", name, myBo)
	}
}

func testGenericDao_GdaoCreateDelete(dao godal.IGenericDao, tableName string, t *testing.T) {
	name := "TestGenericDao_GdaoCreateDelete"
	bo := &MyBo{
		Id:       "1",
		Username: "2",
		Name:     "BO - 3",
		Version:  4,
	}
	if numRows, err := dao.GdaoCreate(tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	gbo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{colId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}

	if numRows, err := dao.GdaoDelete(tableName, gbo); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	gbo, err = dao.GdaoFetchOne(tableName, map[string]interface{}{colId: "1"})
	if err != nil {
		t.Fatalf("%s failed, has error: %e", name, err)
	}
	if gbo != nil {
		t.Fatalf("%s failed, should have nill result, but received: %v", name, gbo)
	}
}

func testGenericDao_GdaoCreateDeleteAll(dao godal.IGenericDao, tableName string, t *testing.T) {
	name := "TestGenericDao_GdaoCreateDeleteAll"
	bo := &MyBo{
		Id:       "1",
		Username: "11",
		Name:     "BO - 1",
		Version:  111,
	}
	if numRows, err := dao.GdaoCreate(tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	bo = &MyBo{
		Id:       "2",
		Username: "22",
		Name:     "BO - 2",
		Version:  222,
	}
	if numRows, err := dao.GdaoCreate(tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	if numRows, err := dao.GdaoDeleteMany(tableName, nil); err != nil || numRows != 2 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	gbo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{colId: "1"})
	if err != nil {
		t.Fatalf("%s failed, has error: %e", name, err)
	}
	if gbo != nil {
		t.Fatalf("%s failed, should have nill result, but received: %v", name, gbo)
	}
}

func testGenericDao_GdaoCreateDeleteMany(dao godal.IGenericDao, tableName string, t *testing.T) {
	name := "TestGenericDao_GdaoCreateDeleteMany"
	totalRows := 10
	for i := 0; i < totalRows; i++ {
		bo := &MyBo{
			Id:       strconv.Itoa(i),
			Username: strconv.Itoa(i + 1),
			Name:     "BO - " + strconv.Itoa(i+2),
			Version:  i + 3,
		}
		if numRows, err := dao.GdaoCreate(tableName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}

	filter := &FilterAnd{
		Filters: []IFilter{
			&FilterFieldValue{Field: colId, Operation: ">=", Value: "1"},
			&FilterFieldValue{Field: colId, Operation: "<=", Value: "8"},
		},
		Operator: "AND",
	}
	if numRows, err := dao.GdaoDeleteMany(tableName, filter); err != nil || numRows != totalRows-2 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	for i := 0; i < totalRows; i++ {
		gbo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{colId: strconv.Itoa(i)})
		if i == 0 || i == totalRows-1 {
			if err != nil || gbo == nil {
				t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
			}
		} else {
			if err != nil {
				t.Fatalf("%s failed, has error: %e", name, err)
			}
			if gbo != nil {
				t.Fatalf("%s failed, should have nill result, but received: %v", name, gbo)
			}
		}
	}
}

func testGenericDao_GdaoFetchAllWithSorting(dao godal.IGenericDao, tableName string, t *testing.T) {
	name := "TestGenericDao_GdaoFetchAllWithSorting"
	numItems := 100
	for i := 1; i <= numItems; i++ {
		bo := &MyBo{
			Id:       fmt.Sprintf("%03d", i),
			Username: strconv.Itoa(i),
			Name:     "BO - " + strconv.Itoa(i),
			Version:  i,
		}
		if numRows, err := dao.GdaoCreate(tableName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}

	gboList, err := dao.GdaoFetchMany(tableName, nil, map[string]int{colId: -1}, 0, 0)
	if err != nil || gboList == nil || len(gboList) != 100 {
		t.Fatalf("%s failed - NumItems: %v / Error: %e", name, len(gboList), err)
	}

	for i, gbo := range gboList {
		if bo := fromGbo(gbo); bo.Id != fmt.Sprintf("%03d", numItems-i) {
			t.Fatalf("%s failed - Expected: %v / Received: %v", name, numItems-i, bo)
		}
	}
}

func testGenericDao_GdaoFetchManyWithPaging(dao godal.IGenericDao, tableName string, t *testing.T) {
	name := "TestGenericDao_GdaoFetchManyWithPaging"
	numItems := 100
	for i := 0; i < numItems; i++ {
		bo := &MyBo{
			Id:       fmt.Sprintf("%03d", i),
			Username: strconv.Itoa(i),
			Name:     "BO - " + strconv.Itoa(i),
			Version:  i,
		}
		if numRows, err := dao.GdaoCreate(tableName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}

	gboList, err := dao.GdaoFetchMany(tableName, &FilterFieldValue{Field: colId, Operation: ">=", Value: "080"}, map[string]int{colId: 1}, 5, 20)
	if err != nil || gboList == nil || len(gboList) != 15 {
		t.Fatalf("%s failed - NumItems: %v / Error: %e", name, len(gboList), err)
	}

	for i, gbo := range gboList {
		if bo := fromGbo(gbo); bo.Id != fmt.Sprintf("%03d", 80+i+5) {
			t.Fatalf("%s failed - Expected: %v / Received: %v", name, 80+i+5, bo)
		}
	}
}

func testGenericDao_GdaoUpdateNotExist(dao godal.IGenericDao, tableName string, t *testing.T) {
	name := "TestGenericDao_GdaoUpdateNotExist"
	bo := &MyBo{
		Id:       "1",
		Username: "2",
		Name:     "BO - 3",
		Version:  4,
	}
	if numRows, err := dao.GdaoUpdate(tableName, bo.ToGbo()); err != nil || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
}

func testGenericDao_GdaoUpdateDuplicated(dao godal.IGenericDao, tableName string, t *testing.T) {
	name := "TestGenericDao_GdaoUpdateDuplicated"
	for i := 0; i < 2; i++ {
		bo := &MyBo{
			Id:       strconv.Itoa(i),
			Username: strconv.Itoa(i),
			Name:     "BO - " + strconv.Itoa(i),
			Version:  1,
		}
		if numRows, err := dao.GdaoCreate(tableName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}
	gbo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{colId: "0"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	myBo := fromGbo(gbo)
	myBo.Username = "1"
	if numRows, err := dao.GdaoUpdate(tableName, myBo.ToGbo()); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
}

func testGenericDao_GdaoUpdate(dao godal.IGenericDao, tableName string, t *testing.T) {
	name := "TestGenericDao_GdaoUpdate"
	for i := 0; i < 3; i++ {
		bo := &MyBo{
			Id:       strconv.Itoa(i),
			Username: strconv.Itoa(i),
			Name:     "BO - " + strconv.Itoa(i),
			Version:  i,
		}
		if numRows, err := dao.GdaoCreate(tableName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}

	bo := &MyBo{
		Id:       "0",
		Username: "100",
		Name:     "BO",
		Version:  100,
	}
	if numRows, err := dao.GdaoUpdate(tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	for i := 0; i < 3; i++ {
		gbo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{colId: strconv.Itoa(i)})
		if err != nil || gbo == nil {
			t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
		}
		if myBo := fromGbo(gbo); myBo == nil {
			t.Fatalf("%s failed - not found: %v", name, i)
		} else if i == 0 && (myBo.Id != bo.Id || myBo.Username != bo.Username || myBo.Name != bo.Name || myBo.Version != bo.Version) {
			t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
		} else if i != 0 && myBo.Version != i {
			t.Fatalf("%s failed - Expected: %v / Received: %v", name, i, myBo.Version)
		}
	}
}

func testGenericDao_GdaoSaveDuplicated_TxModeOff(dao godal.IGenericDao, tableName string, t *testing.T) {
	name := "TestGenericDao_GdaoSaveDuplicated_TxModeOff"
	for i := 1; i <= 3; i++ {
		bo := &MyBo{
			Id:       strconv.Itoa(i),
			Username: strconv.Itoa(i),
			Name:     "BO - " + strconv.Itoa(i),
			Version:  i,
		}
		if numRows, err := dao.GdaoSave(tableName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
		gbo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{colId: strconv.Itoa(i)})
		if err != nil || gbo == nil {
			t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
		}
		if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
			t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
		}
	}

	// save new one with duplicated key
	bo := &MyBo{
		Id:       strconv.Itoa(0),
		Username: strconv.Itoa(1),
		Name:     "BO - " + strconv.Itoa(0),
		Version:  0,
	}
	if numRows, err := dao.GdaoSave(tableName, bo.ToGbo()); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	// save existing one with duplicated key
	bo = &MyBo{
		Id:       strconv.Itoa(1),
		Username: strconv.Itoa(2),
		Name:     "BO - " + strconv.Itoa(1),
		Version:  1,
	}
	if numRows, err := dao.GdaoSave(tableName, bo.ToGbo()); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
}

func testGenericDao_GdaoSaveDuplicated_TxModeOn(dao godal.IGenericDao, tableName string, t *testing.T) {
	name := "TestGenericDao_GdaoSaveDuplicated_TxModeOn"
	for i := 1; i <= 3; i++ {
		bo := &MyBo{
			Id:       strconv.Itoa(i),
			Username: strconv.Itoa(i),
			Name:     "BO - " + strconv.Itoa(i),
			Version:  i,
		}
		if numRows, err := dao.GdaoSave(tableName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
		gbo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{colId: strconv.Itoa(i)})
		if err != nil || gbo == nil {
			t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
		}
		if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
			t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
		}
	}

	// save new one with duplicated key
	bo := &MyBo{
		Id:       strconv.Itoa(0),
		Username: strconv.Itoa(1),
		Name:     "BO - " + strconv.Itoa(0),
		Version:  0,
	}
	if numRows, err := dao.GdaoSave(tableName, bo.ToGbo()); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	// save existing one with duplicated key
	bo = &MyBo{
		Id:       strconv.Itoa(1),
		Username: strconv.Itoa(2),
		Name:     "BO - " + strconv.Itoa(1),
		Version:  1,
	}
	if numRows, err := dao.GdaoSave(tableName, bo.ToGbo()); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
}

func testGenericDao_GdaoSave_TxModeOff(dao godal.IGenericDao, tableName string, t *testing.T) {
	name := "TestGenericDao_GdaoSave_TxModeOff"
	bo := &MyBo{
		Id:       "1",
		Username: "2",
		Name:     "BO - 3",
		Version:  4,
	}
	if numRows, err := dao.GdaoSave(tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	gbo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{colId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}

	bo.Name = "BO"
	bo.Version = 10
	if numRows, err := dao.GdaoSave(tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	gbo, err = dao.GdaoFetchOne(tableName, map[string]interface{}{colId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}
}

func testGenericDao_GdaoSave_TxModeOn(dao godal.IGenericDao, tableName string, t *testing.T) {
	name := "TestGenericDao_GdaoSave_TxModeOn"
	bo := &MyBo{
		Id:       "1",
		Username: "2",
		Name:     "BO - 3",
		Version:  4,
	}
	if numRows, err := dao.GdaoSave(tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	gbo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{colId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}

	bo.Name = "BO"
	bo.Version = 10
	if numRows, err := dao.GdaoSave(tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	gbo, err = dao.GdaoFetchOne(tableName, map[string]interface{}{colId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}
}
