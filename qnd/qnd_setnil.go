package main

import (
	"fmt"
	"time"

	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/sql"
)

const (
	tableUser = "test_user"

	// table columns
	colUserId       = "uid"
	colUserUsername = "uusername"
	colUserName     = "uname"
	colUserVersion  = "uversion"
	colUserActived  = "uactived"

	// BO fields
	fieldUserId       = "id"
	fieldUserUsername = "username"
	fieldUserName     = "name"
	fieldUserVersion  = "version"
	fieldUserActived  = "actived"
)

func main() {
	bo := godal.NewGenericBo()
	bo.GboSetAttr(fieldUserId, "1")
	bo.GboSetAttr(fieldUserUsername, "btnguyen2k")
	bo.GboSetAttr(fieldUserName, "Nguyễn Bá Thành")
	bo.GboSetAttr(fieldUserVersion, time.Now().Unix())
	bo.GboSetAttr(fieldUserActived, 1) // convention: 1=true / 0=false

	bo.GboSetAttr(fieldUserVersion, godal.NilValue)
	fmt.Printf("%s\n", bo.GboToJsonUnsafe())

	rowMapper := &sql.GenericRowMapperSql{
		// it is a good idea to normalize table column names and BO field names
		// in this case, we use "lower case transformation" rule to normalize table column and BO field names
		NameTransformation: sql.NameTransfLowerCase,
		GboFieldToColNameTranslator: map[string]map[string]interface{}{
			// {generic bo field -> database table column} mapping for tableUser
			tableUser: {
				fieldUserId:       colUserId,
				fieldUserUsername: colUserUsername,
				fieldUserName:     colUserName,
				fieldUserVersion:  colUserVersion,
				fieldUserActived:  colUserActived,
			},
			// mapping for other tables
		},
		ColNameToGboFieldTranslator: map[string]map[string]interface{}{
			// {database table column -> generic bo field} mapping for tableUser
			tableUser: {
				colUserId:       fieldUserId,
				colUserUsername: fieldUserUsername,
				colUserName:     fieldUserName,
				colUserVersion:  fieldUserVersion,
				colUserActived:  fieldUserActived,
			},
			// mapping for other tables
		},
		ColumnsListMap: map[string][]string{
			// all database table columns of tableUser
			tableUser: {colUserId, colUserUsername, colUserName, colUserVersion, colUserActived},

			// ...other tables
		},
	}

	bo.GboSetAttr(fieldUserVersion, godal.NilValue)
	row, err := rowMapper.ToRow(tableUser, bo)
	fmt.Println(row, err)
}
