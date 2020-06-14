package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/prom"

	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/dynamodb"
)

// convenient function to create prom.AwsDynamodbConnect instance
func createAwsDynamodbConnect(region string) *prom.AwsDynamodbConnect {
	// AWS credentials are provided via environment variables
	cfg := &aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewEnvCredentials(),
	}
	timeoutMs := 10000
	adc, err := prom.NewAwsDynamodbConnect(cfg, nil, nil, timeoutMs)
	if err != nil {
		panic(err)
	}
	return adc
}

// convenient function to create MyGenericDaoDynamodb instance
func createMyGenericDaoDynamodb(adc *prom.AwsDynamodbConnect, rowMapper godal.IRowMapper) godal.IGenericDao {
	dao := &MyGenericDaoDynamodb{}
	dao.GenericDaoDynamodb = dynamodb.NewGenericDaoDynamodb(adc, godal.NewAbstractGenericDao(dao))
	dao.SetRowMapper(rowMapper)
	return dao
}

const (
	tableUser         = "test_user"
	fieldUserId       = "id"
	fieldUserUsername = "username"
	fieldUserName     = "name"
	fieldUserVersion  = "version"
	fieldUserActived  = "actived"
)

type MyGenericDaoDynamodb struct {
	*dynamodb.GenericDaoDynamodb
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter
func (dao *MyGenericDaoDynamodb) GdaoCreateFilter(tableName string, bo godal.IGenericBo) interface{} {
	if tableName == tableUser {
		// should match all primary keys
		return map[string]interface{}{
			fieldUserId: bo.GboGetAttrUnsafe(fieldUserId, reddo.TypeString),
		}
	}

	// primary key filtering for other database tables
	// ...

	return nil
}

func main() {
	// create new prom.AwsDynamodbConnect connecting to SouthEast region
	adc := createAwsDynamodbConnect("ap-southeast-1")

	rowMapper := &dynamodb.GenericRowMapperDynamodb{
		ColumnsListMap: map[string][]string{
			tableUser: {fieldUserId}, // primary keys of tableUser or UserBo
			// column lists for other BOs/tables
		},
	}

	// create new MyGenericDaoDynamodb
	myDao := createMyGenericDaoDynamodb(adc, rowMapper)

	bo := godal.NewGenericBo()
	bo.GboSetAttr(fieldUserId, "1")
	bo.GboSetAttr(fieldUserUsername, "btnguyen2k")
	bo.GboSetAttr(fieldUserName, "Nguyễn Bá Thành")
	bo.GboSetAttr(fieldUserVersion, time.Now().Unix())
	bo.GboSetAttr(fieldUserActived, true)

	{
		// CREATE
		_, err := myDao.GdaoCreate(tableUser, bo)
		fmt.Printf("Creating user [%s]...: %e\n", bo.GboToJsonUnsafe(), err)
	}

	{
		// READ
		filterBo := godal.NewGenericBo()
		filterBo.GboSetAttr(fieldUserId, "1")
		myBo, err := myDao.GdaoFetchOne(tableUser, myDao.GdaoCreateFilter(tableUser, filterBo))
		fmt.Printf("Fetched user [%s]: %e\n", myBo.GboToJsonUnsafe(), err)
	}

	{
		// UPDATE
		bo.GboSetAttr(fieldUserVersion, nil)
		bo.GboSetAttr("new_field", "a value")
		bo.GboSetAttr(fieldUserActived, false)
		_, err := myDao.GdaoUpdate(tableUser, bo)
		fmt.Printf("Updated user [%s]: %e\n", bo.GboToJsonUnsafe(), err)

		_, err = myDao.GdaoSave(tableUser, bo)
		fmt.Printf("Saved user [%s]: %e\n", bo.GboToJsonUnsafe(), err)
	}

	{
		// DELETE
		_, err := myDao.GdaoDelete(tableUser, bo)
		fmt.Printf("Deleted user [%s]: %e\n", bo.GboToJsonUnsafe(), err)
	}
}
