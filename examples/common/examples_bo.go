package common

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/btnguyen2k/consu/reddo"

	"github.com/btnguyen2k/godal"
)

var (
	// SEP defines separator string used for displaying purpose
	SEP = "================================================================================"

	// TIMEZONE defines the timezone used for all examples
	TIMEZONE = "Asia/Ho_Chi_Minh"
)

// PrintApp prints a BoApp's info to stdout.
func PrintApp(app *BoApp) {
	fmt.Printf("\tApp [%s] info: %v\n", app.Id, string(app.ToJson()))
	fmt.Printf("\t\t%s (%T): %v\n", "Id", app.Id, app.Id)
	fmt.Printf("\t\t%s (%T): %v\n", "Description", app.Description, app.Description)
	fmt.Printf("\t\t%s (%T): %v\n", "ValBool", app.ValBool, app.ValBool)
	fmt.Printf("\t\t%s (%T): %v\n", "ValInt", app.ValInt, app.ValInt)
	fmt.Printf("\t\t%s (%T): %v\n", "ValFloat", app.ValFloat, app.ValFloat)
	fmt.Printf("\t\t%s (%T): %v\n", "ValString", app.ValString, app.ValString)
	fmt.Printf("\t\t%s (%T): %v\n", "ValTime", app.ValTime, app.ValTime)
	fmt.Printf("\t\t%s (%T): %v\n", "ValTimeZ", app.ValTimeZ, app.ValTimeZ)
	fmt.Printf("\t\t%s (%T): %v\n", "ValDate", app.ValDate, app.ValDate)
	fmt.Printf("\t\t%s (%T): %v\n", "ValDateZ", app.ValDateZ, app.ValDateZ)
	fmt.Printf("\t\t%s (%T): %v\n", "ValDatetime", app.ValDatetime, app.ValDatetime)
	fmt.Printf("\t\t%s (%T): %v\n", "ValDatetimeZ", app.ValDatetimeZ, app.ValDatetimeZ)
	fmt.Printf("\t\t%s (%T): %v\n", "ValTimestamp", app.ValTimestamp, app.ValTimestamp)
	fmt.Printf("\t\t%s (%T): %v\n", "ValTimestampZ", app.ValTimestampZ, app.ValTimestampZ)
	fmt.Printf("\t\t%s (%T): %v\n", "ValList", app.ValList, app.ValList)
	fmt.Printf("\t\t%s (%T): %v\n", "ValMap", app.ValMap, app.ValMap)
}

// BoApp defines business object app
type BoApp struct {
	Id            string                 `json:"id"`
	Description   string                 `json:"val_desc"`
	ValBool       bool                   `json:"val_bool"`
	ValInt        int                    `json:"val_int"`
	ValFloat      float64                `json:"val_float"`
	ValString     string                 `json:"val_string"`
	ValTime       time.Time              `json:"val_time"`
	ValTimeZ      time.Time              `json:"val_timez"`
	ValDate       time.Time              `json:"val_date"`
	ValDateZ      time.Time              `json:"val_datez"`
	ValDatetime   time.Time              `json:"val_datetime"`
	ValDatetimeZ  time.Time              `json:"val_datetimez"`
	ValTimestamp  time.Time              `json:"val_timestamp"`
	ValTimestampZ time.Time              `json:"val_timestampz"`
	ValList       []interface{}          `json:"val_list"`
	ValMap        map[string]interface{} `json:"val_map"`
}

// ToJson serializes BoApp to JSON string.
func (app *BoApp) ToJson() []byte {
	js, _ := json.Marshal(app)
	return js
}

// ToGenericBo transforms BoApp to godal.IGenericBo
func (app *BoApp) ToGenericBo() godal.IGenericBo {
	gbo := godal.NewGenericBo()
	gbo.GboSetAttr("id", app.Id)
	gbo.GboSetAttr("val_desc", app.Description)
	// BO's bool is stored as CHAR(1)
	if app.ValBool {
		gbo.GboSetAttr("val_bool", "1")
	} else {
		gbo.GboSetAttr("val_bool", "0")
	}
	gbo.GboSetAttr("val_int", app.ValInt)
	gbo.GboSetAttr("val_float", app.ValFloat)
	gbo.GboSetAttr("val_string", app.ValString)
	gbo.GboSetAttr("val_time", app.ValTime)
	gbo.GboSetAttr("val_timez", app.ValTimeZ)
	gbo.GboSetAttr("val_date", app.ValDate)
	gbo.GboSetAttr("val_datez", app.ValDateZ)
	gbo.GboSetAttr("val_datetime", app.ValDatetime)
	gbo.GboSetAttr("val_datetimez", app.ValDatetimeZ)
	gbo.GboSetAttr("val_timestamp", app.ValTimestamp)
	gbo.GboSetAttr("val_timestampz", app.ValTimestampZ)
	gbo.GboSetAttr("val_list", app.ValList)
	gbo.GboSetAttr("val_map", app.ValMap)
	return gbo
}

// FromGenericBo imports BoApp's data from a godal.IGenericBo instance.
func (app *BoApp) FromGenericBo(gbo godal.IGenericBo) *BoApp {
	if v := gbo.GboGetAttrUnsafe("id", reddo.TypeString); v != nil {
		app.Id = v.(string)
	}
	if v := gbo.GboGetAttrUnsafe("val_desc", reddo.TypeString); v != nil {
		app.Description = v.(string)
	}
	if v := gbo.GboGetAttrUnsafe("val_bool", reddo.TypeBool); v != nil {
		app.ValBool = v.(bool)
	}
	if v := gbo.GboGetAttrUnsafe("val_int", reddo.TypeInt); v != nil {
		app.ValInt = int(v.(int64))
	}
	if v := gbo.GboGetAttrUnsafe("val_float", reddo.TypeFloat); v != nil {
		app.ValFloat = v.(float64)
	}
	if v := gbo.GboGetAttrUnsafe("val_string", reddo.TypeString); v != nil {
		app.ValString = v.(string)
	}
	app.ValTime, _ = gbo.GboGetTimeWithLayout("val_time", time.RFC3339Nano)
	app.ValTimeZ, _ = gbo.GboGetTimeWithLayout("val_timez", time.RFC3339Nano)
	app.ValDate, _ = gbo.GboGetTimeWithLayout("val_date", time.RFC3339Nano)
	app.ValDateZ, _ = gbo.GboGetTimeWithLayout("val_datez", time.RFC3339Nano)
	app.ValDatetime, _ = gbo.GboGetTimeWithLayout("val_datetime", time.RFC3339Nano)
	app.ValDatetimeZ, _ = gbo.GboGetTimeWithLayout("val_datetimez", time.RFC3339Nano)
	app.ValTimestamp, _ = gbo.GboGetTimeWithLayout("val_timestamp", time.RFC3339Nano)
	app.ValTimestampZ, _ = gbo.GboGetTimeWithLayout("val_timestampz", time.RFC3339Nano)
	if v := gbo.GboGetAttrUnsafe("val_list", reddo.TypeString); v != nil {
		json.Unmarshal([]byte(v.(string)), &(app.ValList))
	}
	if v := gbo.GboGetAttrUnsafe("val_map", reddo.TypeString); v != nil {
		json.Unmarshal([]byte(v.(string)), &(app.ValMap))
	}
	return app
}

// IDaoApp defines DAO APIs for apps
type IDaoApp interface {
	// EnableTxMode enables/disables transaction mode
	EnableTxMode(txMode bool)

	// Delete removes an app from database store.
	Delete(bo *BoApp) (bool, error)

	// Create persists a new app to database store. If the app already existed, this function returns (false, nil)
	Create(bo *BoApp) (bool, error)

	// Get finds an app by id & fetches it from database store.
	Get(id string) (*BoApp, error)

	// GetAll retrieves all available apps from database store and returns them as a list.
	GetAll() ([]*BoApp, error)

	// GetN retries apps from database store with paging.
	GetN(startOffset, numRows int) ([]*BoApp, error)

	// Update modifies an existing app in the database store. If the application does not exist in database, this function returns (false, nil).
	Update(bo *BoApp) (bool, error)

	// Upsert performs "update-or-create" on the specified app: the app is updated/modified if it already existed, otherwise it is created.
	Upsert(bo *BoApp) (bool, error)
}
