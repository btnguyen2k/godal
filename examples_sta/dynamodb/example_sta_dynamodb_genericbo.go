package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/prom"

	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/dynamodb"
)

// convenient function to create prom.AwsDynamodbConnect instance
func createAwsDynamodbConnectGeneric(region string) *prom.AwsDynamodbConnect {
	awsRegion := strings.ReplaceAll(os.Getenv("AWS_REGION"), `"`, "")
	awsAccessKeyId := strings.ReplaceAll(os.Getenv("AWS_ACCESS_KEY_ID"), `"`, "")
	awsSecretAccessKey := strings.ReplaceAll(os.Getenv("AWS_SECRET_ACCESS_KEY"), `"`, "")
	if awsRegion == "" || awsAccessKeyId == "" || awsSecretAccessKey == "" {
		panic("Please define env AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and optionally AWS_DYNAMODB_ENDPOINT")
	}
	cfg := &aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewEnvCredentials(),
	}
	if awsDynamodbEndpoint := strings.ReplaceAll(os.Getenv("AWS_DYNAMODB_ENDPOINT"), `"`, ""); awsDynamodbEndpoint != "" {
		cfg.Endpoint = aws.String(awsDynamodbEndpoint)
		if strings.HasPrefix(awsDynamodbEndpoint, "http://") {
			cfg.DisableSSL = aws.Bool(true)
		}
	}
	adc, err := prom.NewAwsDynamodbConnect(cfg, nil, nil, 10000)
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
	tableUserGeneric         = "test_user"
	fieldUserIdGeneric       = "id"
	fieldUserUsernameGeneric = "username"
	fieldUserNameGeneric     = "name"
	fieldUserVersionGeneric  = "version"
	fieldUserActivedGeneric  = "actived"
)

type MyGenericDaoDynamodb struct {
	*dynamodb.GenericDaoDynamodb
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter
func (dao *MyGenericDaoDynamodb) GdaoCreateFilter(tableName string, bo godal.IGenericBo) interface{} {
	if tableName == tableUserGeneric {
		// should match all primary keys
		return map[string]interface{}{
			fieldUserIdGeneric: bo.GboGetAttrUnsafe(fieldUserIdGeneric, reddo.TypeString),
		}
	}

	// primary key filtering for other database tables
	// ...

	return nil
}

func main() {
	// create new prom.AwsDynamodbConnect connecting to SouthEast region
	adc := createAwsDynamodbConnectGeneric("ap-southeast-1")

	rowMapper := &dynamodb.GenericRowMapperDynamodb{
		ColumnsListMap: map[string][]string{
			tableUserGeneric: {fieldUserIdGeneric}, // primary keys of tableUserGeneric or UserBo
			// column lists for other BOs/tables
		},
	}

	// create new MyGenericDaoDynamodb
	myDao := createMyGenericDaoDynamodb(adc, rowMapper)

	bo := godal.NewGenericBo()
	bo.GboSetAttr(fieldUserIdGeneric, "1")
	bo.GboSetAttr(fieldUserUsernameGeneric, "btnguyen2k")
	bo.GboSetAttr(fieldUserNameGeneric, "Nguyễn Bá Thành")
	bo.GboSetAttr(fieldUserVersionGeneric, time.Now().Unix())
	bo.GboSetAttr(fieldUserActivedGeneric, true)

	{
		// CREATE
		_, err := myDao.GdaoCreate(tableUserGeneric, bo)
		fmt.Printf("Creating user [%s]...: %e\n", bo.GboToJsonUnsafe(), err)
	}

	{
		// READ
		filterBo := godal.NewGenericBo()
		filterBo.GboSetAttr(fieldUserIdGeneric, "1")
		myBo, err := myDao.GdaoFetchOne(tableUserGeneric, myDao.GdaoCreateFilter(tableUserGeneric, filterBo))
		fmt.Printf("Fetched user [%s]: %e\n", myBo.GboToJsonUnsafe(), err)
	}

	{
		fmt.Scanln()

		// UPDATE
		bo.GboSetAttr(fieldUserVersionGeneric, godal.NilValue)
		bo.GboSetAttr("new_field", "a value")
		bo.GboSetAttr(fieldUserActivedGeneric, false)
		_, err := myDao.GdaoUpdate(tableUserGeneric, bo)
		fmt.Printf("Updated user [%s]: %e\n", bo.GboToJsonUnsafe(), err)
		fmt.Scanln()

		_, err = myDao.GdaoSave(tableUserGeneric, bo)
		fmt.Printf("Saved user [%s]: %e\n", bo.GboToJsonUnsafe(), err)
		fmt.Scanln()
	}

	{
		// DELETE
		_, err := myDao.GdaoDelete(tableUserGeneric, bo)
		fmt.Printf("Deleted user [%s]: %e\n", bo.GboToJsonUnsafe(), err)
	}
}
