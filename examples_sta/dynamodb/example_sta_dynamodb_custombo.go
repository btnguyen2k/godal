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
func createAwsDynamodbConnect(region string) *prom.AwsDynamodbConnect {
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

// convenient function to create UserDaoDynamodb instance
func createUserDaoDynamodb(adc *prom.AwsDynamodbConnect, tableName string, rowMapper godal.IRowMapper) IUserDao {
	dao := &UserDaoDynamodb{tableName: tableName}
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

// UserBo is a custom BO that encapsulates an application user object
type UserBo struct {
	Id       string `json:"id"`
	Username string `json:"username"`
	Name     string `json:"name"`
	Version  int    `json:"version"`
	Actived  bool   `json:"actived"`
}

// IUserDao defines interface to access user storage
type IUserDao interface {
	Create(bo *UserBo) (bool, error)
	Get(id string) (*UserBo, error)
	Update(bo *UserBo) (bool, error)
	Save(bo *UserBo) (bool, error)
	Delete(bo *UserBo) (bool, error)
}

// UserDaoDynamodb is AWS DynamoDB implementation of IUserDao
type UserDaoDynamodb struct {
	*dynamodb.GenericDaoDynamodb
	tableName string
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter
func (dao *UserDaoDynamodb) GdaoCreateFilter(_ string, bo godal.IGenericBo) interface{} {
	return map[string]interface{}{
		fieldUserId: bo.GboGetAttrUnsafe(fieldUserId, reddo.TypeString),
	}
}

// toGbo converts a UserBo to godal.IGenericBo
// (*) it is recommended that DAO provides method to convert BO to IGenericBo
func (dao *UserDaoDynamodb) toGbo(bo *UserBo) godal.IGenericBo {
	if bo == nil {
		return nil
	}
	gbo := godal.NewGenericBo()

	gbo.GboSetAttr(fieldUserId, bo.Id)
	gbo.GboSetAttr(fieldUserUsername, bo.Username)
	gbo.GboSetAttr(fieldUserName, bo.Name)
	gbo.GboSetAttr(fieldUserVersion, bo.Version)
	gbo.GboSetAttr(fieldUserActived, bo.Actived)

	// another way
	// js, _ := json.Marshal(bo)
	// gbo.GboFromJson(js)

	return gbo
}

// toBo converts a godal.IGenericBo to UserBo
// (*) it is recommended that DAO provides method to convert IGenericBo to BO
func (dao *UserDaoDynamodb) toBo(gbo godal.IGenericBo) *UserBo {
	if gbo == nil {
		return nil
	}
	bo := UserBo{}

	bo.Id = gbo.GboGetAttrUnsafe(fieldUserId, reddo.TypeString).(string)             // assume field "id" is not nil
	bo.Username = gbo.GboGetAttrUnsafe(fieldUserUsername, reddo.TypeString).(string) // assume field "username" is not nil
	bo.Name = gbo.GboGetAttrUnsafe(fieldUserName, reddo.TypeString).(string)         // assume field "name" is not nil
	bo.Version = int(gbo.GboGetAttrUnsafe(fieldUserVersion, reddo.TypeInt).(int64))  // assume field "version" is not nil
	bo.Actived = gbo.GboGetAttrUnsafe(fieldUserActived, reddo.TypeBool).(bool)       // assume field "actived" is not nil

	// another way
	// if err := json.Unmarshal(gbo.GboToJsonUnsafe(), &bo); err != nil {
	// 	panic(err)
	// }

	return &bo
}

// Create implements IUserDao.Create
func (dao *UserDaoDynamodb) Create(bo *UserBo) (bool, error) {
	numRows, err := dao.GdaoCreate(dao.tableName, dao.toGbo(bo))
	return numRows > 0, err
}

// Get implements IUserDao.Get
func (dao *UserDaoDynamodb) Get(id string) (*UserBo, error) {
	filterGbo := godal.NewGenericBo()
	filterGbo.GboSetAttr(fieldUserId, id)
	gbo, err := dao.GdaoFetchOne(dao.tableName, dao.GdaoCreateFilter(dao.tableName, filterGbo))
	return dao.toBo(gbo), err
}

// Update implements IUserDao.Update
func (dao *UserDaoDynamodb) Update(bo *UserBo) (bool, error) {
	numRows, err := dao.GdaoUpdate(dao.tableName, dao.toGbo(bo))
	return numRows > 0, err
}

// Save implements IUserDao.Save
func (dao *UserDaoDynamodb) Save(bo *UserBo) (bool, error) {
	numRows, err := dao.GdaoSave(dao.tableName, dao.toGbo(bo))
	return numRows > 0, err
}

// Delete implements IUserDao.Delete
func (dao *UserDaoDynamodb) Delete(bo *UserBo) (bool, error) {
	numRows, err := dao.GdaoDelete(dao.tableName, dao.toGbo(bo))
	return numRows > 0, err
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

	bo := &UserBo{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thành Nguyễn",
		Version:  int(time.Now().Unix()),
		Actived:  true,
	}

	{
		// CREATE
		_, err := daoUser.Create(bo)
		fmt.Printf("Creating user [%v]...: %e\n", bo, err)
	}

	{
		// READ
		user, err := daoUser.Get("1")
		fmt.Printf("Fetched user [%v]: %e\n", user, err)
	}

	{
		// UPDATE
		bo.Version = 103
		bo.Actived = false
		_, err := daoUser.Update(bo)
		fmt.Printf("Updated user [%v]: %e\n", bo, err)

		bo.Id = "1"
		bo.Version = 301
		bo.Username = "thanhn"
		_, err = daoUser.Save(bo)
		fmt.Printf("Saved user [%v]: %e\n", bo, err)
	}

	{
		// DELETE
		_, err := daoUser.Delete(bo)
		fmt.Printf("Deleted user [%v]: %e\n", bo, err)
	}
}
