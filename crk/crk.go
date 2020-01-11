package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/techoner/gophp/serialize"
)

const (
	USERNAME = "root"
	//PASSWORD = ""//game123456
	NETWORK  = "tcp"
	SERVER   = "localhost"
	PORT     = 3306
	DATABASE = "cj655"
)

var PASSWORD string = ""
var DB *sql.DB
var err error
var maxConnections int
var waitDBNotBusyCount int
var waitDBNotBusyTimeout int
var loc *time.Location

type ServerData struct {
	GameId   int64 `db:"game_id"`
	ServerId int64 `db:"game_server_id"`
}

type ServerDetailData struct {
	Username string `json:"username"`
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
}

func main() {

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

	date := time.Now().AddDate(0, 0, -1)

	loc, _ = time.LoadLocation("Local")

	serverDatas := getServerDatas()

	for i := 0; i < 1; i++ {
		startDate := date.AddDate(0, 0, -i)

		theTime := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, loc)

		startTime := theTime.Unix()
		endTime := startTime + 86399

		for _, serverData := range serverDatas {

			if serverData.ServerId != 41270 {
				continue
			}

			serverDetailDatas := getServerDetailDatas(serverData, startTime, endTime)

			userIds := getUserIds(serverDetailDatas)

			createRoleKeepData := getCreateRoleKeepData(serverData, userIds, theTime)

			SmartPrint(createRoleKeepData)
		}

	}

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

func getCreateRoleKeepData(serverData ServerData, userIds []string, date time.Time) (createRoleKeepData CreateRoleKeepData) {
	createNum := len(userIds)

	keepNum1 := getServerKeepNum(1, date, userIds)

	keepNum3 := getServerKeepNum(3, date, userIds)

	keepNum7 := getServerKeepNum(7, date, userIds)

	keepNum14 := getServerKeepNum(14, date, userIds)

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
	createRoleKeepData.KeepNum30 = keepNum30
	createRoleKeepData.KeepNum60 = keepNum60
	createRoleKeepData.KeepNum90 = keepNum90

	return
}

func getServerKeepNum(day int, date time.Time, userIds []string) (keepNum int) {

	startDate := date.AddDate(0, 0, +day)

	theTime := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, loc)

	startTime := theTime.Unix()
	endTime := startTime + 86399

	querySql := fmt.Sprintf(
		`SELECT count(distinct user_id) login_count FROM gc_user_play_data WHERE (user_id in ( %s )) AND (login_time BETWEEN %d AND %d)`,
		strings.Join(userIds, ","),
		startTime,
		endTime,
	)

	row := DB.QueryRow(querySql)
	row.Scan(&keepNum)

	return
}

func getUserIds(serverDetailDatas []ServerDetailData) (userIds []string) {

	var usernames []string

	for _, serverDetailData := range serverDetailDatas {
		usernames = append(usernames, serverDetailData.Username)
	}

	querySql := fmt.Sprintf("SELECT user_id FROM gc_user WHERE username in ( '%s' )", strings.Join(usernames, "','"))

	rows, err := DB.Query(querySql)

	if err != nil {
		fmt.Println(err)
		return
	}

	var userId string

	for rows.Next() {
		rows.Scan(&userId)
		userIds = append(userIds, userId)
	}

	defer func() {
		rows.Close()
	}()

	return
}

func getServerDetailDatas(serverData ServerData, startTime int64, endTime int64) (serverDetailDatas []ServerDetailData) {

	where := fmt.Sprintf("server_id = %d AND (add_time BETWEEN %d AND %d)", serverData.ServerId, startTime, endTime)

	where2, _ := serialize.Marshal(where)

	where3 := string(where2)

	field := "distinct username"

	url := fmt.Sprintf("http://dj.cj655.com/api.php?m=player&a=admin_role_array7&where=%s&field=%s", where3, field)

	resp, err := http.Get(url)

	if err != nil {
		fmt.Println(err)
		return
	}

	body, _ := ioutil.ReadAll(resp.Body)

	_ = json.Unmarshal(body, &serverDetailDatas)

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
