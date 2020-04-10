package main

import (
	"crypto/md5"
	"database/sql"
	"flag"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
)

var (
	USERNAME string = "root"
	PASSWORD string = "" //game123456
	NETWORK  string = "tcp"
	SERVER   string = "localhost"
	PORT     int    = 3306
	DATABASE string = "cj655"
)

var (
	USERNAME2 string = "cj655"
	PASSWORD2 string = "game123456" //game123456
	NETWORK2  string = "tcp"
	SERVER2   string = "120.132.31.31"
	PORT2     int    = 3306
	DATABASE2 string = "cj655"
)

var DB *sql.DB
var DB2 *sql.DB
var RC redis.Conn
var err error
var maxConnections int
var waitDBNotBusyCount int
var waitDBNotBusyTimeout int
var loc *time.Location
var totalCount int64 = 0
var totalSuccessCount int64 = 0
var totalErrorCount int64 = 0
var typeEffective string
var typeCharge string
var serverId int64
var myDate string

type ServerData struct {
	GameId   int64 `db:"game_id"`
	ServerId int64 `db:"game_server_id"`
}

type ServerDetailData struct {
	UserId          string `db:"user_id"`
	FirstChargeTime int64  `db:"first_charge_time"`
}

type CreateRoleKeepData struct {
	Id        int64
	Date      string
	DateTime  int64
	GameId    int64
	ServerId  int64
	CreateNum int
	KeepNum1  int
	KeepNum3  int
	KeepNum7  int
	KeepNum14 int
	KeepNum20 int
	KeepNum30 int
	KeepNum60 int
	KeepNum90 int
}

type EffectiveRoleKeepData struct {
	Id           int64
	Date         string
	DateTime     int64
	GameId       int64
	ServerId     int64
	EffectiveNum int
	KeepNum1     int
	KeepNum3     int
	KeepNum7     int
	KeepNum14    int
	KeepNum20    int
	KeepNum30    int
	KeepNum60    int
	KeepNum90    int
}

type ChargeRoleKeepData struct {
	Id        int64
	Date      string
	DateTime  int64
	GameId    int64
	ServerId  int64
	ChargeNum int
	KeepNum1  int
	KeepNum3  int
	KeepNum7  int
	KeepNum14 int
	KeepNum20 int
	KeepNum30 int
	KeepNum60 int
	KeepNum90 int
}

func init() {
	myOS := os.Getenv("OS")
	if myOS == "Windows_NT" {
		PASSWORD = ""
	} else {
		PASSWORD = "game123456"
	}

	DB = openDB()
	DB2 = openDB2()

	loc, _ = time.LoadLocation("Local")

	flag.StringVar(&typeEffective, "type-effective", "off", "有效数模式")
	flag.StringVar(&typeCharge, "type-charge", "off", "付费数模式")
	flag.StringVar(&myDate, "my-date", "", "汇总日期")
	flag.Int64Var(&serverId, "server-id", 0, "区服id")
	flag.Parse()

	RC, err = redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Println("Connect to redis error", err)
		return
	}
}

func main() {

	//fmt.Println(serverId)
	//os.Exit(0)

	date := time.Now().AddDate(0, 0, -2)

	fmt.Println(fmt.Sprintf("Task begin StartDate:%s EndDate:%s RunDate:%s", date.AddDate(0, 0, -92).Format("2006-01-02"), date.Format("2006-01-02"), time.Now().Format("2006-01-02 15:04:05")))

	totalStartTime := time.Now().Unix()

	serverDatas := getServerDatas()

	//left over three days
	for i := 0; i < 93; i++ {

		//whether database is busy
		for {
			if isDBBusy() {
				DB.Close()
				waitDBNotBusyCount++
				waitTime := time.Second * time.Duration(math.Pow(5, float64(waitDBNotBusyCount)))
				fmt.Println(fmt.Sprintf("Database is busy,WaitCount:%d WaitTime:%v", waitDBNotBusyCount, waitTime))
				time.Sleep(waitTime)
				openDB()
			} else {
				waitDBNotBusyCount = 0
				break
			}
		}

		startDate := date.AddDate(0, 0, -i)

		theTime := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, loc)

		startTime := theTime.Unix()
		endTime := startTime + 86399

		if myDate != "" && theTime.Format("2006-01-02") != myDate {
			continue
		}

		for _, serverData := range serverDatas {

			if serverId > 0 && serverData.ServerId != serverId {
				continue
			}

			var userIds []string

			if typeCharge == "on" {
				key := fmt.Sprintf("%x", md5.Sum([]byte(fmt.Sprintf("%v%d%d", serverData, startTime, startTime))))
				keyExist, _ := redis.Bool(RC.Do("HEXISTS", "cj655_crk_charge_user_map", key))
				if keyExist {
					r, _ := redis.String(RC.Do("HGET", "cj655_crk_charge_user_map", key))
					if r != "" {
						userIds = strings.Split(r, ",")
					}
				} else {
					serverDetailDatas := getServerDetailDatas(serverData, startTime, endTime)
					userIds = getUserIds(serverDetailDatas)
					//RC.Do("SET", key, strings.Join(userIds, ","))
					RC.Do("HSET", "cj655_crk_charge_user_map", key, strings.Join(userIds, ","))
				}
			} else {
				serverDetailDatas := getServerDetailDatas(serverData, startTime, endTime)
				userIds = getUserIds(serverDetailDatas)
			}

			//fmt.Println(userIds)
			//fmt.Println(len(userIds))
			//os.Exit(0)

			if len(userIds) == 0 {
				continue
			}

			//fmt.Println(userIds)

			if typeEffective == "on" {
				effectiveRoleKeepData := getEffectiveRoleKeepData(serverData, userIds, theTime)

				if id := isExistEffectiveRoleKeepData(effectiveRoleKeepData); id > 0 {
					effectiveRoleKeepData.Id = id
				}

				//SmartPrint(effectiveRoleKeepData)

				saveEffectiveRoleKeepData(effectiveRoleKeepData)

			} else if typeCharge == "on" {
				chargeRoleKeepData := getChargeRoleKeepData(serverData, userIds, theTime)

				// fmt.Println(chargeRoleKeepData)
				// os.Exit(0)

				if id := isExistChargeRoleKeepData(chargeRoleKeepData); id > 0 {
					chargeRoleKeepData.Id = id
				}

				saveChargeRoleKeepData(chargeRoleKeepData)
			} else {
				createRoleKeepData := getCreateRoleKeepData(serverData, userIds, theTime)

				if id := isExistCreateRoleKeepData(createRoleKeepData); id > 0 {
					createRoleKeepData.Id = id
				}

				//SmartPrint(createRoleKeepData)

				saveCreateRoleKeepData(createRoleKeepData)
			}

		}

		taskEndTime := time.Now().Unix()
		fmt.Println(fmt.Sprintf("Task %s is compeleted,ElapsedTime:%s", theTime.Format("2006-01-02"), resolveSecond(taskEndTime-totalStartTime)))
	}

	totalEndTime := time.Now().Unix()

	fmt.Println(fmt.Sprintf("All task is compeleted,SuccessRow:%d ErrorRow:%d TotalRow:%d Time:%s",
		totalSuccessCount, totalErrorCount, totalCount, resolveSecond(totalEndTime-totalStartTime)))

}

func openDB() (DB *sql.DB) {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s", USERNAME, PASSWORD, NETWORK, SERVER, PORT, DATABASE)
	DB, err = sql.Open("mysql", dsn)

	if err != nil {
		panic(fmt.Sprintf("Open mysql failed,Error:%v\n", err))
	}

	DB.SetConnMaxLifetime(100 * time.Second) //最大连接周期，超过时间的连接就close
	DB.SetMaxOpenConns(100)                  //设置最大连接数
	DB.SetMaxIdleConns(16)                   //设置闲置连接数

	if maxConnections == 0 {
		var variableName string
		row := DB.QueryRow(`show variables like "max_connections"`)
		row.Scan(&variableName, &maxConnections)
	}

	return
}

func openDB2() (DB2 *sql.DB) {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s", USERNAME2, PASSWORD2, NETWORK2, SERVER2, PORT2, DATABASE2)
	DB2, err = sql.Open("mysql", dsn)

	if err != nil {
		panic(fmt.Sprintf("Open mysql failed,Error:%v\n", err))
	}

	DB2.SetConnMaxLifetime(100 * time.Second) //最大连接周期，超过时间的连接就close
	DB2.SetMaxOpenConns(100)                  //设置最大连接数
	DB2.SetMaxIdleConns(16)                   //设置闲置连接数

	return
}

func isDBBusy() bool {
	if getCurrentDBConnections() > maxConnections/2 {
		return true
	}
	return false
}

func getCurrentDBConnections() (processlistCount int) {
	row := DB.QueryRow(`SELECT COUNT(ID) processlist_count from information_schema.processlist`)
	row.Scan(&processlistCount)
	return
}

func getChargeRoleKeepData(serverData ServerData, userIds []string, date time.Time) (chargeRoleKeepData ChargeRoleKeepData) {
	chargeNum := len(userIds)

	keepNum1 := getServerKeepNum(1, date, userIds)

	keepNum3 := getServerKeepNum(3, date, userIds)

	keepNum7 := getServerKeepNum(7, date, userIds)

	keepNum14 := getServerKeepNum(14, date, userIds)

	keepNum20 := getServerKeepNum(20, date, userIds)

	keepNum30 := getServerKeepNum(30, date, userIds)

	keepNum60 := getServerKeepNum(60, date, userIds)

	keepNum90 := getServerKeepNum(90, date, userIds)

	chargeRoleKeepData.Date = date.Format("2006-01-02")
	chargeRoleKeepData.DateTime = date.Unix()
	chargeRoleKeepData.GameId = serverData.GameId
	chargeRoleKeepData.ServerId = serverData.ServerId
	chargeRoleKeepData.ChargeNum = chargeNum
	chargeRoleKeepData.KeepNum1 = keepNum1
	chargeRoleKeepData.KeepNum3 = keepNum3
	chargeRoleKeepData.KeepNum7 = keepNum7
	chargeRoleKeepData.KeepNum14 = keepNum14
	chargeRoleKeepData.KeepNum20 = keepNum20
	chargeRoleKeepData.KeepNum30 = keepNum30
	chargeRoleKeepData.KeepNum60 = keepNum60
	chargeRoleKeepData.KeepNum90 = keepNum90

	return
}

func isExistChargeRoleKeepData(chargeRoleKeepData ChargeRoleKeepData) (id int64) {
	querySql := fmt.Sprintf(`SELECT id FROM gc_charge_role_keep WHERE date_time = %d AND game_id = %d AND server_id = %d LIMIT 1`,
		chargeRoleKeepData.DateTime,
		chargeRoleKeepData.GameId,
		chargeRoleKeepData.ServerId,
	)

	row := DB.QueryRow(querySql)
	row.Scan(&id)

	return
}

func saveChargeRoleKeepData(chargeRoleKeepData ChargeRoleKeepData) {
	var err error
	var querySql string

	if chargeRoleKeepData.Id > 0 {
		querySql = `UPDATE gc_charge_role_keep SET 
		charge_num=?,keep_num_1=?,keep_num_3=?,keep_num_7=?,keep_num_14=?,keep_num_30=?,keep_num_60=?,keep_num_90=? 
		WHERE id=?`
		_, err = DB.Exec(
			querySql,
			chargeRoleKeepData.ChargeNum,
			chargeRoleKeepData.KeepNum1,
			chargeRoleKeepData.KeepNum3,
			chargeRoleKeepData.KeepNum7,
			chargeRoleKeepData.KeepNum14,
			chargeRoleKeepData.KeepNum30,
			chargeRoleKeepData.KeepNum60,
			chargeRoleKeepData.KeepNum90,
			chargeRoleKeepData.Id,
		)
	} else {
		querySql = `insert INTO gc_charge_role_keep
		(date,date_time,game_id,server_id,charge_num,keep_num_1,keep_num_3,keep_num_7,keep_num_14,keep_num_20,keep_num_30,keep_num_60,keep_num_90)
		values(?,?,?,?,?,?,?,?,?,?,?,?,?)`
		_, err = DB.Exec(
			querySql,
			chargeRoleKeepData.Date,
			chargeRoleKeepData.DateTime,
			chargeRoleKeepData.GameId,
			chargeRoleKeepData.ServerId,
			chargeRoleKeepData.ChargeNum,
			chargeRoleKeepData.KeepNum1,
			chargeRoleKeepData.KeepNum3,
			chargeRoleKeepData.KeepNum7,
			chargeRoleKeepData.KeepNum14,
			chargeRoleKeepData.KeepNum20,
			chargeRoleKeepData.KeepNum30,
			chargeRoleKeepData.KeepNum60,
			chargeRoleKeepData.KeepNum90,
		)
	}

	if err != nil {
		totalErrorCount++
		fmt.Println(fmt.Sprintf("Data:%v Error:%v Sql:%s", chargeRoleKeepData, err, querySql))
	} else {
		totalSuccessCount++
	}

	totalCount++
}

func getEffectiveRoleKeepData(serverData ServerData, userIds []string, date time.Time) (effectiveRoleKeepData EffectiveRoleKeepData) {
	effectiveNum := len(userIds)

	keepNum1 := getServerKeepNum(1, date, userIds)

	keepNum3 := getServerKeepNum(3, date, userIds)

	keepNum7 := getServerKeepNum(7, date, userIds)

	keepNum14 := getServerKeepNum(14, date, userIds)

	keepNum20 := getServerKeepNum(20, date, userIds)

	keepNum30 := getServerKeepNum(30, date, userIds)

	keepNum60 := getServerKeepNum(60, date, userIds)

	keepNum90 := getServerKeepNum(90, date, userIds)

	effectiveRoleKeepData.Date = date.Format("2006-01-02")
	effectiveRoleKeepData.DateTime = date.Unix()
	effectiveRoleKeepData.GameId = serverData.GameId
	effectiveRoleKeepData.ServerId = serverData.ServerId
	effectiveRoleKeepData.EffectiveNum = effectiveNum
	effectiveRoleKeepData.KeepNum1 = keepNum1
	effectiveRoleKeepData.KeepNum3 = keepNum3
	effectiveRoleKeepData.KeepNum7 = keepNum7
	effectiveRoleKeepData.KeepNum14 = keepNum14
	effectiveRoleKeepData.KeepNum20 = keepNum20
	effectiveRoleKeepData.KeepNum30 = keepNum30
	effectiveRoleKeepData.KeepNum60 = keepNum60
	effectiveRoleKeepData.KeepNum90 = keepNum90

	return
}

func isExistEffectiveRoleKeepData(effectiveRoleKeepData EffectiveRoleKeepData) (id int64) {
	querySql := fmt.Sprintf(`SELECT id FROM gc_effective_role_keep WHERE date_time = %d AND game_id = %d AND server_id = %d LIMIT 1`,
		effectiveRoleKeepData.DateTime,
		effectiveRoleKeepData.GameId,
		effectiveRoleKeepData.ServerId,
	)

	row := DB.QueryRow(querySql)
	row.Scan(&id)

	return
}

func saveEffectiveRoleKeepData(effectiveRoleKeepData EffectiveRoleKeepData) {
	var err error
	var querySql string

	if effectiveRoleKeepData.Id > 0 {
		querySql = `UPDATE gc_effective_role_keep SET 
		effective_num=?,keep_num_1=?,keep_num_3=?,keep_num_7=?,keep_num_14=?,keep_num_30=?,keep_num_60=?,keep_num_90=? 
		WHERE id=?`
		_, err = DB.Exec(
			querySql,
			effectiveRoleKeepData.EffectiveNum,
			effectiveRoleKeepData.KeepNum1,
			effectiveRoleKeepData.KeepNum3,
			effectiveRoleKeepData.KeepNum7,
			effectiveRoleKeepData.KeepNum14,
			effectiveRoleKeepData.KeepNum30,
			effectiveRoleKeepData.KeepNum60,
			effectiveRoleKeepData.KeepNum90,
			effectiveRoleKeepData.Id,
		)
	} else {
		querySql = `insert INTO gc_effective_role_keep
		(date,date_time,game_id,server_id,effective_num,keep_num_1,keep_num_3,keep_num_7,keep_num_14,keep_num_30,keep_num_60,keep_num_90)
		values(?,?,?,?,?,?,?,?,?,?,?,?)`
		_, err = DB.Exec(
			querySql,
			effectiveRoleKeepData.Date,
			effectiveRoleKeepData.DateTime,
			effectiveRoleKeepData.GameId,
			effectiveRoleKeepData.ServerId,
			effectiveRoleKeepData.EffectiveNum,
			effectiveRoleKeepData.KeepNum1,
			effectiveRoleKeepData.KeepNum3,
			effectiveRoleKeepData.KeepNum7,
			effectiveRoleKeepData.KeepNum14,
			effectiveRoleKeepData.KeepNum30,
			effectiveRoleKeepData.KeepNum60,
			effectiveRoleKeepData.KeepNum90,
		)
	}

	if err != nil {
		totalErrorCount++
		fmt.Println(fmt.Sprintf("Data:%v Error:%v Sql:%s", effectiveRoleKeepData, err, querySql))
	} else {
		totalSuccessCount++
	}

	totalCount++
}

func getCreateRoleKeepData(serverData ServerData, userIds []string, date time.Time) (createRoleKeepData CreateRoleKeepData) {
	createNum := len(userIds)

	keepNum1 := getServerKeepNum(1, date, userIds)

	keepNum3 := getServerKeepNum(3, date, userIds)

	keepNum7 := getServerKeepNum(7, date, userIds)

	keepNum14 := getServerKeepNum(14, date, userIds)

	keepNum20 := getServerKeepNum(20, date, userIds)

	keepNum30 := getServerKeepNum(30, date, userIds)

	keepNum60 := getServerKeepNum(60, date, userIds)

	keepNum90 := getServerKeepNum(90, date, userIds)

	createRoleKeepData.Date = date.Format("2006-01-02")
	createRoleKeepData.DateTime = date.Unix()
	createRoleKeepData.GameId = serverData.GameId
	createRoleKeepData.ServerId = serverData.ServerId
	createRoleKeepData.CreateNum = createNum
	createRoleKeepData.KeepNum1 = keepNum1
	createRoleKeepData.KeepNum3 = keepNum3
	createRoleKeepData.KeepNum7 = keepNum7
	createRoleKeepData.KeepNum14 = keepNum14
	createRoleKeepData.KeepNum20 = keepNum20
	createRoleKeepData.KeepNum30 = keepNum30
	createRoleKeepData.KeepNum60 = keepNum60
	createRoleKeepData.KeepNum90 = keepNum90

	return
}

func isExistCreateRoleKeepData(createRoleKeepData CreateRoleKeepData) (id int64) {
	querySql := fmt.Sprintf(`SELECT id FROM gc_create_role_keep WHERE date_time = %d AND game_id = %d AND server_id = %d LIMIT 1`,
		createRoleKeepData.DateTime,
		createRoleKeepData.GameId,
		createRoleKeepData.ServerId,
	)

	row := DB.QueryRow(querySql)
	row.Scan(&id)

	return
}

func saveCreateRoleKeepData(createRoleKeepData CreateRoleKeepData) {
	var err error
	var querySql string

	if createRoleKeepData.Id > 0 {
		querySql = `UPDATE gc_create_role_keep SET 
		create_num=?,keep_num_1=?,keep_num_3=?,keep_num_7=?,keep_num_14=?,keep_num_30=?,keep_num_60=?,keep_num_90=? 
		WHERE id=?`
		_, err = DB.Exec(
			querySql,
			createRoleKeepData.CreateNum,
			createRoleKeepData.KeepNum1,
			createRoleKeepData.KeepNum3,
			createRoleKeepData.KeepNum7,
			createRoleKeepData.KeepNum14,
			createRoleKeepData.KeepNum30,
			createRoleKeepData.KeepNum60,
			createRoleKeepData.KeepNum90,
			createRoleKeepData.Id,
		)
	} else {
		querySql = `insert INTO gc_create_role_keep
		(date,date_time,game_id,server_id,create_num,keep_num_1,keep_num_3,keep_num_7,keep_num_14,keep_num_30,keep_num_60,keep_num_90)
		values(?,?,?,?,?,?,?,?,?,?,?,?)`
		_, err = DB.Exec(
			querySql,
			createRoleKeepData.Date,
			createRoleKeepData.DateTime,
			createRoleKeepData.GameId,
			createRoleKeepData.ServerId,
			createRoleKeepData.CreateNum,
			createRoleKeepData.KeepNum1,
			createRoleKeepData.KeepNum3,
			createRoleKeepData.KeepNum7,
			createRoleKeepData.KeepNum14,
			createRoleKeepData.KeepNum30,
			createRoleKeepData.KeepNum60,
			createRoleKeepData.KeepNum90,
		)
	}

	if err != nil {
		totalErrorCount++
		fmt.Println(fmt.Sprintf("Data:%v Error:%v Sql:%s", createRoleKeepData, err, querySql))
	} else {
		totalSuccessCount++
	}

	totalCount++
}

func getServerKeepNum(day int, date time.Time, userIds []string) (keepNum int) {

	startDate := date.AddDate(0, 0, +day)

	theTime := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, loc)

	startTime := theTime.Unix()
	endTime := startTime + 86399

	querySql := fmt.Sprintf(
		`SELECT count(distinct user_id) keep_num FROM gc_user_play_data WHERE (user_id in ( %s )) AND (login_time BETWEEN %d AND %d)`,
		strings.Join(userIds, ","),
		startTime,
		endTime,
	)

	// if typeCharge == "on" {
	// 	querySql = fmt.Sprintf(
	// 		`SELECT count(distinct user_id) keep_num FROM gc_order WHERE (user_id in ( %s )) AND (create_time BETWEEN %d AND %d) AND channel = 1 AND status = 1`,
	// 		strings.Join(userIds, ","),
	// 		startTime,
	// 		endTime,
	// 	)
	// }

	row := DB.QueryRow(querySql)
	row.Scan(&keepNum)

	return
}

func getUserIds(serverDetailDatas []ServerDetailData) (userIds []string) {

	for _, serverDetailData := range serverDetailDatas {
		userIds = append(userIds, serverDetailData.UserId)
	}

	return
}

func getServerDetailDatas(serverData ServerData, startTime int64, endTime int64) (serverDetailDatas []ServerDetailData) {

	var querySql string
	querySql = fmt.Sprintf(
		`SELECT distinct u.user_id 
		FROM gc_user_role as ur LEFT JOIN gc_user as u on ur.username = u.username 
		WHERE ur.game_id = %d AND ur.server_id = %d AND (ur.add_time BETWEEN %d AND %d) AND (u.reg_time BETWEEN %d AND %d)`,
		serverData.GameId, serverData.ServerId, startTime, endTime, startTime, endTime,
	)

	if typeEffective == "on" {
		querySql = fmt.Sprintf(
			`SELECT distinct u.user_id 
			FROM gc_user_role as ur LEFT JOIN gc_user as u on ur.username = u.username 
			WHERE ur.game_id = %d AND ur.server_id = %d AND (ur.dabiao_time BETWEEN %d AND %d) AND (u.reg_time BETWEEN %d AND %d) AND ur.is_effective = 1`,
			serverData.GameId, serverData.ServerId, startTime, endTime, startTime, endTime,
		)
	}

	if typeCharge == "on" {
		querySql = fmt.Sprintf(
			`SELECT user_id,MIN(create_time) first_charge_time FROM gc_order 
			WHERE game_id = %d AND game_server_id = %d AND channel = 1 AND status = 1 GROUP BY user_id HAVING first_charge_time BETWEEN %d AND %d`,
			serverData.GameId, serverData.ServerId, startTime, endTime,
		)
	}

	//fmt.Println(querySql)
	var rows *sql.Rows
	var err error
	if typeCharge == "on" {
		rows, err = DB.Query(querySql)
	} else {
		rows, err = DB2.Query(querySql)
	}

	if err != nil {
		fmt.Println(fmt.Sprintf("Get server data detail error, Error:%s", err))
	}

	serverDetailData := new(ServerDetailData)

	for rows.Next() {
		rows.Scan(&serverDetailData.UserId, &serverDetailData.FirstChargeTime)
		serverDetailDatas = append(serverDetailDatas, *serverDetailData)
	}

	defer func() {
		rows.Close()
	}()

	return

}

func getServerDatas() (serverDatas []ServerData) {

	querySql := "SELECT game_id,game_server_id FROM gc_game_server"

	rows, err := DB.Query(querySql)

	if err != nil {
		panic(fmt.Sprintf("Get server data error, Error:%s", err))
	}

	serverData := new(ServerData)

	for rows.Next() {
		rows.Scan(&serverData.GameId, &serverData.ServerId)
		serverDatas = append(serverDatas, *serverData)
	}

	defer func() {
		rows.Close()
	}()

	return
}

func SmartPrint(i interface{}) {
	var kv = make(map[string]interface{})
	vValue := reflect.ValueOf(i)
	vType := reflect.TypeOf(i)
	for i := 0; i < vValue.NumField(); i++ {
		kv[vType.Field(i).Name] = vValue.Field(i)
	}
	for k, v := range kv {
		fmt.Print(k)
		fmt.Print(":")
		fmt.Print(v)
		fmt.Println()
	}
}

func resolveSecond(second int64) (time string) {

	minute := second / 60

	hour := minute / 60

	minute = minute % 60

	second = second - hour*3600 - minute*60

	time = fmt.Sprintf("%d:%d:%d", hour, minute, second)

	//fmt.Println(time)

	return
}
