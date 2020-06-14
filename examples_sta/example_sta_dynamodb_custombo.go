package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/prom"

	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/dynamodb"
)

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

func createUserDaoDynamodb(adc *prom.AwsDynamodbConnect, tableName string, rowMapper godal.IRowMapper) *UserDaoDynamodb {
	dao := &UserDaoDynamodb{tableName: tableName}
	dao.GenericDaoDynamodb = dynamodb.NewGenericDaoDynamodb(adc, godal.NewAbstractGenericDao(dao))
	dao.SetRowMapper(rowMapper)
	return dao
}

const tableUser = "tbl_user"
const fieldUserId = "id"

type UserBo struct {
	Id       string `json:"id"`
	Username string `json:"username"`
	Name     string `json:"name"`
	Version  int    `json:"version"`
	Actived  int    `json:"actived"`
}

// UserDaoDynamodb is DAO for UserBo
type UserDaoDynamodb struct {
	*dynamodb.GenericDaoDynamodb
	tableName string
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter
func (dao *UserDaoDynamodb) GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{} {
	return map[string]interface{}{
		fieldUserId: bo.GboGetAttrUnsafe(fieldUserId, reddo.TypeString),
	}
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

	// create new UserDaoDynamodb
	daoUser := createUserDaoDynamodb(adc, tableUser, rowMapper)

	fmt.Println(daoUser)
}
