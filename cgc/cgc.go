package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type GameData struct {
	GameId          int64  `db:"game_id"`
	GameName        string `db:"game_name"`
	OpenServerCount int    `db:"open_server_count"`
}

type ChannelData struct {
	ChannelId int64 `db:"channel_id"`
	Type      int   `db:"type"`
}

type PresidentChannelData struct {
	ChannelId       string  `json:"channel_id"`
	GradeMoney      string  `json:"grade_money"`
	ImGradeMoney    string  `json:"im_grade_money"`
	JlNum           string  `json:"jl_num"`
	UniqueRegNum    string  `json:"unique_reg_num"`
	PerSettleMoney  int     //渠道单价
	EffectiveNum    int     //游戏有效数
	GameSettleMoney int     //游戏投放成本
	RegNum          int     //游戏注册数
	RegUsers        []int64 //游戏注册用户
}

type ChannelGameCountData struct {
	Id              int64
	Date            string
	DateTime        int64
	GameId          int64
	GameName        string
	OpenServerCount int
	RegNum          int
	EffectiveNum    int
	SettleMoney     int
}

type ChannelGameCountUserData struct {
	Id       int64
	Date     string
	DateTime int64
	GameId   int64
	UserId   int64
}

var (
	USERNAME string = "root"
	PASSWORD string = "" //game123456
	NETWORK  string = "tcp"
	SERVER   string = "localhost"
	PORT     int    = 3306
	DATABASE string = "cj655"
)

var (
	USERNAME2 string = "admin"
	PASSWORD2 string = "game123456" //game123456
	NETWORK2  string = "tcp"
	SERVER2   string = "120.132.31.222"
	PORT2     int    = 3306
	DATABASE2 string = "cj655"
)

var (
	USERNAME3 string = "cj655"
	PASSWORD3 string = "game123456" //game123456
	NETWORK3  string = "tcp"
	SERVER3   string = "117.27.139.18"
	PORT3     int    = 3306
	DATABASE3 string = "cj655"
)

var (
	USERNAME4 string = "cj655"
	PASSWORD4 string = "game123456" //game123456
	NETWORK4  string = "tcp"
	SERVER4   string = "120.132.31.31"
	PORT4     int    = 3306
	DATABASE4 string = "cj655"
)

var DB *sql.DB
var DB2 *sql.DB
var DB3 *sql.DB
var DB4 *sql.DB
var loc *time.Location
var months []string

func init() {
	myOS := os.Getenv("OS")
	if myOS == "Windows_NT" {
		PASSWORD = ""
	} else {
		PASSWORD = "game123456"
	}

	DB = openDB()
	DB2 = openDB2()
	DB3 = openDB3()
	DB4 = openDB4()

	loc, _ = time.LoadLocation("Local")

	months = append(months, time.Now().Format("2006-01"))

	//1号统计上月数据
	day := time.Now().Day()
	if day == 1 {
		curMonth := int(time.Now().Month())
		lastMonth := curMonth - 1
		curYear := int(time.Now().Year())
		if lastMonth <= 0 {
			lastMonth = 12
			curYear = curYear - 1
		}
		months = append(months, fmt.Sprintf("%d-%02d", curYear, lastMonth))
	}

	var currentMonth string
	flag.StringVar(&currentMonth, "month", "", "当前的月份")
	flag.Parse()

	if currentMonth != "" {
		months = strings.Split(currentMonth, ",")
	}
}

func main() {
	allStartTime := time.Now().Unix()

	//获取投放渠道
	exclusiveChannelIds := getExclusiveChannelIds()

	for _, month := range months {
		startTask(month, exclusiveChannelIds)
	}

	allEndTime := time.Now().Unix()

	fmt.Println(fmt.Sprintf("All task is compeleted,Time:%s", resolveSecond(allEndTime-allStartTime)))
}

func startTask(month string, exclusiveChannelIds []string) {
	theTime, _ := time.ParseInLocation("2006-01", month, loc)
	startTime := theTime.Unix()
	endTimeDate := theTime.AddDate(0, 1, -1)
	endTime := endTimeDate.Unix() + 86399

	fmt.Println(fmt.Sprintf("Task:%s begin StartDate:%s EndDate:%s RunDate:%s", month, theTime.Format("2006-01-02"), endTimeDate.Format("2006-01-02"), time.Now().Format("2006-01-02 15:04:05")))

	taskStartTime := time.Now().Unix()

	gameDatas := getGameDatas(startTime, endTime)

	for _, gameData := range gameDatas {

		channelDatas := getChannelDatas(gameData, exclusiveChannelIds)

		presidentChannelIds := getPresidentChannelIdsByChannelData(channelDatas)

		//fmt.Println(len(presidentChannelIds))
		//presidentChannelIds = []string{"1"}

		presidentChannelDatas := getPresidentChannelDatas(presidentChannelIds, theTime.Format("2006-01"), gameData, startTime, endTime)

		channelGameCountUserDatas := getChannelGameCountUserDatas(presidentChannelDatas, gameData, theTime)

		channelGameCountData := getChannelGameCountData(presidentChannelDatas, gameData, theTime)

		//汇总渠道数据
		for _, presidentChannelData := range presidentChannelDatas {
			saveChannelGameCountData2(presidentChannelData, gameData, theTime)
		}

		saveChannelGameCountUserDatas(channelGameCountUserDatas)

		saveChannelGameCountData(channelGameCountData)

		//break
	}

	taskEndTime := time.Now().Unix()

	fmt.Println(fmt.Sprintf("Task:%s is compeleted,Time:%s", month, resolveSecond(taskEndTime-taskStartTime)))
}

func isExistChannelGameCountData2(presidentChannelData PresidentChannelData, gameData GameData, date time.Time) (id int64) {
	querySql := fmt.Sprintf(
		`SELECT id FROM gc_channel_game_count2 WHERE date_time = %d AND game_id = %d AND channel_id = %s LIMIT 1`,
		date.Unix(), gameData.GameId, presidentChannelData.ChannelId,
	)

	row := DB.QueryRow(querySql)
	row.Scan(&id)

	return
}

func saveChannelGameCountData2(presidentChannelData PresidentChannelData, gameData GameData, date time.Time) {
	var err error
	var querySql string

	if id := isExistChannelGameCountData2(presidentChannelData, gameData, date); id > 0 {
		querySql = `UPDATE gc_channel_game_count2 SET open_server_count = ?,reg_num = ?,effective_num = ?,settle_money = ? WHERE id = ?`
		_, err = DB.Exec(querySql, gameData.OpenServerCount, presidentChannelData.RegNum, presidentChannelData.EffectiveNum, presidentChannelData.GameSettleMoney, id)
	} else {
		querySql = `INSERT INTO gc_channel_game_count2 (date,date_time,channel_id,game_id,game_name,open_server_count,reg_num,effective_num,settle_money) values(?,?,?,?,?,?,?,?,?)`
		_, err = DB.Exec(querySql, date.Format("2006-01"), date.Unix(), presidentChannelData.ChannelId, gameData.GameId, gameData.GameName,
			gameData.OpenServerCount, presidentChannelData.RegNum, presidentChannelData.EffectiveNum, presidentChannelData.GameSettleMoney)
	}

	if err != nil {
		fmt.Println(fmt.Sprintf("Error:%v Sql:%s", err, querySql))
	}
}

func isExistChannelGameCountUserData(channelGameCountUserData ChannelGameCountUserData) (id int64) {
	querySql := fmt.Sprintf(`SELECT id FROM gc_channel_game_count_user WHERE date_time = %d AND game_id = %d AND user_id = %d LIMIT 1`,
		channelGameCountUserData.DateTime, channelGameCountUserData.GameId, channelGameCountUserData.UserId)

	row := DB.QueryRow(querySql)
	row.Scan(&id)

	return
}

func saveChannelGameCountUserDatas(channelGameCountUserDatas []ChannelGameCountUserData) {
	for _, channelGameCountUserData := range channelGameCountUserDatas {
		if id := isExistChannelGameCountUserData(channelGameCountUserData); id == 0 {
			querySql := `INSERT INTO gc_channel_game_count_user (date,date_time,game_id,user_id) values(?,?,?,?)`
			DB.Exec(querySql, channelGameCountUserData.Date, channelGameCountUserData.DateTime, channelGameCountUserData.GameId, channelGameCountUserData.UserId)
		}
	}
}

func getChannelGameCountUserDatas(presidentChannelDatas []PresidentChannelData, gameData GameData, date time.Time) (channelGameCountUserDatas []ChannelGameCountUserData) {

	channelGameCountUserData := new(ChannelGameCountUserData)
	for _, presidentChannelData := range presidentChannelDatas {
		for _, userId := range presidentChannelData.RegUsers {
			channelGameCountUserData.Date = date.Format("2006-01")
			channelGameCountUserData.DateTime = date.Unix()
			channelGameCountUserData.GameId = gameData.GameId
			channelGameCountUserData.UserId = userId

			channelGameCountUserDatas = append(channelGameCountUserDatas, *channelGameCountUserData)
		}
	}

	return
}

func isExistChannelGameCountData(channelGameCountData ChannelGameCountData) (id int64) {
	querySql := fmt.Sprintf(`SELECT id FROM gc_channel_game_count WHERE date_time = %d AND game_id = %d LIMIT 1`, channelGameCountData.DateTime, channelGameCountData.GameId)

	row := DB.QueryRow(querySql)
	row.Scan(&id)

	return
}

func saveChannelGameCountData(channelGameCountData ChannelGameCountData) {
	var err error
	var querySql string

	if id := isExistChannelGameCountData(channelGameCountData); id > 0 {
		querySql = `UPDATE gc_channel_game_count SET open_server_count = ?,reg_num = ?,effective_num = ?,settle_money = ? WHERE id = ?`
		_, err = DB.Exec(querySql, channelGameCountData.OpenServerCount, channelGameCountData.RegNum, channelGameCountData.EffectiveNum,
			channelGameCountData.SettleMoney, id)
	} else {
		querySql = `INSERT INTO gc_channel_game_count (date,date_time,game_id,game_name,open_server_count,reg_num,effective_num,settle_money) values(?,?,?,?,?,?,?,?)`
		_, err = DB.Exec(querySql, channelGameCountData.Date, channelGameCountData.DateTime, channelGameCountData.GameId, channelGameCountData.GameName, channelGameCountData.OpenServerCount,
			channelGameCountData.RegNum, channelGameCountData.EffectiveNum, channelGameCountData.SettleMoney)
	}

	if err != nil {
		fmt.Println(fmt.Sprintf("Data:%v Error:%v Sql:%s", channelGameCountData, err, querySql))
	}
}

func getChannelGameCountData(presidentChannelDatas []PresidentChannelData, gameData GameData, date time.Time) (channelGameCountData ChannelGameCountData) {

	for _, presidentChannelData := range presidentChannelDatas {
		channelGameCountData.SettleMoney += presidentChannelData.GameSettleMoney
		channelGameCountData.RegNum += presidentChannelData.RegNum
		channelGameCountData.EffectiveNum += presidentChannelData.EffectiveNum
	}

	channelGameCountData.Date = date.Format("2006-01")
	channelGameCountData.DateTime = date.Unix()
	channelGameCountData.GameId = gameData.GameId
	channelGameCountData.GameName = gameData.GameName
	channelGameCountData.OpenServerCount = gameData.OpenServerCount

	return
}

func getPresidentChannelDatas(presidentChannelIds []string, month string, gameData GameData, startTime int64, endTime int64) (presidentChannelDatas []PresidentChannelData) {

	//fmt.Println(strings.Join(presidentChannelIds, ","))
	//os.Exit(0)

	resp, err := http.PostForm("https://admin.cj655.com/api.php?m=channelpublic&a=get_channel_month_data", url.Values{
		"month":       {month},
		"channel_ids": {strings.Join(presidentChannelIds, ",")},
		"field":       {"channel_id,unique_reg_num,jl_num,grade_money,im_grade_money"},
		"api_key":     {"TbjoLfLhnikp92hyd8dx0ozCcEipII2Z"},
	})

	if err != nil {
		fmt.Println(err)
		return
	}

	body, _ := ioutil.ReadAll(resp.Body)

	//fmt.Println(string(body))
	//os.Exit(0)

	_ = json.Unmarshal(body, &presidentChannelDatas)

	//计算PerSettleMoney 新增计数用户*20+渠道评级对应的价格*有效数
	//计算PerSettleMoney 从2020-06起:新增计数用户*20+综合评级对应的价格*有效数
	for index, presidentChannelData := range presidentChannelDatas {

		JlNum, _ := strconv.Atoi(presidentChannelData.JlNum)

		GradeMoney, _ := strconv.Atoi(presidentChannelData.GradeMoney)

		ImGradeMoney, _ := strconv.Atoi(presidentChannelData.ImGradeMoney)

		UniqueRegNum, _ := strconv.Atoi(presidentChannelData.UniqueRegNum)

		var SettleMoney int
		//判断是否2020-06月之前
		if startTime < 1590940800 {
			SettleMoney = JlNum*20 + GradeMoney/100*UniqueRegNum
		} else {
			SettleMoney = JlNum*20 + ImGradeMoney/100*UniqueRegNum
		}

		if UniqueRegNum > 0 {
			presidentChannelDatas[index].PerSettleMoney = SettleMoney / UniqueRegNum
		}

	}

	//计算当前游戏有效数
	for index, presidentChannelData := range presidentChannelDatas {
		querySql := fmt.Sprintf(`SELECT count(username) effective_num FROM gc_user_role 
		WHERE is_effective = 1 AND dabiao_time BETWEEN %d AND %d AND game_id = %d AND main_channel_id = %s`, startTime, endTime, gameData.GameId, presidentChannelData.ChannelId)

		row := DB4.QueryRow(querySql)
		var effectiveNum int
		row.Scan(&effectiveNum)

		presidentChannelDatas[index].EffectiveNum = effectiveNum
	}

	//计算游戏投放成本
	for index, presidentChannelData := range presidentChannelDatas {
		presidentChannelDatas[index].GameSettleMoney = presidentChannelData.EffectiveNum * presidentChannelData.PerSettleMoney
	}

	//获取渠道游戏注册用户
	for index, presidentChannelData := range presidentChannelDatas {
		querySql := fmt.Sprintf(`SELECT distinct user_id FROM gc_user 
		WHERE reg_time BETWEEN %d AND %d AND game_id = %d AND main_channel_id = %s`, startTime, endTime, gameData.GameId, presidentChannelData.ChannelId)

		//fmt.Println(querySql)

		rows, _ := DB3.Query(querySql)

		var userId int64
		for rows.Next() {
			rows.Scan(&userId)
			presidentChannelDatas[index].RegUsers = append(presidentChannelDatas[index].RegUsers, userId)
		}
	}

	//计算游戏渠道注册数
	for index, presidentChannelData := range presidentChannelDatas {
		presidentChannelDatas[index].RegNum = len(presidentChannelData.RegUsers)
	}

	return
}

func getPresidentChannelIdsByChannelData(channelDatas []ChannelData) (presidentChannelIds []string) {
	for _, channelData := range channelDatas {
		presidentChannelId := getPresidentChannelIdByChannelData(channelData)
		presidentChannelIds = append(presidentChannelIds, presidentChannelId)

		//break
	}

	return
}

func getPresidentChannelIdByChannelData(channelData ChannelData) (presidentChannelId string) {
	if channelData.Type == 1 {
		presidentChannelId = strconv.FormatInt(channelData.ChannelId, 10)
	}
	if channelData.Type == 2 {
		querySql := fmt.Sprintf("SELECT parent_id FROM gc_president_pid WHERE child_id = %d", channelData.ChannelId)
		row := DB2.QueryRow(querySql)
		row.Scan(&presidentChannelId)
	}
	if channelData.Type == 3 {
		querySql := fmt.Sprintf("SELECT p2.parent_id FROM gc_president_pid AS p1 LEFT JOIN gc_president_pid AS p2 ON p1.parent_id = p2.child_id WHERE p1.child_id = %d", channelData.ChannelId)
		row := DB2.QueryRow(querySql)
		row.Scan(&presidentChannelId)
	}
	return
}

func getExclusiveChannelIds() (exclusiveChannelIds []string) {
	channelIds := getPidsByType(1, 1)

	for _, channelId := range channelIds {
		exclusiveChannelIds = append(exclusiveChannelIds, strconv.Itoa(channelId))
	}

	return
}

func getChannelDatas(gameData GameData, exclusiveChannelIds []string) (channelDatas []ChannelData) {
	//去除虚假引流渠道
	querySql := fmt.Sprintf(`SELECT DISTINCT cag.channel_id,c.type 
	FROM gc_channel_apply_game AS cag LEFT JOIN gc_channel AS c ON cag.channel_id = c.channel_id 
	WHERE cag.game_id = %d AND cag.package_status = 2 AND c.deal_type <> 2 AND cag.channel_id NOT IN ('%s')`, gameData.GameId, strings.Join(exclusiveChannelIds, "','"))

	//fmt.Println(querySql)

	rows, err := DB2.Query(querySql)

	if err != nil {
		fmt.Println(err)
		return
	}

	channelData := new(ChannelData)
	for rows.Next() {
		rows.Scan(&channelData.ChannelId, &channelData.Type)
		channelDatas = append(channelDatas, *channelData)
	}

	defer func() {
		rows.Close()
	}()

	return
}

func getPidsByType(channelId int, channelType int) (channelIds []int) {
	if channelType == 3 {
		channelIds = append(channelIds, channelId)
	}
	if channelType == 2 {
		querySql := fmt.Sprintf("SELECT child_id FROM gc_president_pid WHERE parent_id = %d", channelId)
		rows, _ := DB2.Query(querySql)
		var childId int
		for rows.Next() {
			rows.Scan(&childId)
			channelIds = append(channelIds, childId)
		}

		channelIds = append(channelIds, channelId)
	}
	if channelType == 1 {
		querySql := fmt.Sprintf("SELECT child_id FROM gc_president_pid WHERE parent_id = %d", channelId)
		rows, _ := DB2.Query(querySql)
		var childId int
		for rows.Next() {
			rows.Scan(&childId)
			channelIds = append(channelIds, childId)
		}
		if len(channelIds) > 0 {
			for _, value := range channelIds {
				querySql := fmt.Sprintf("SELECT child_id FROM gc_president_pid WHERE parent_id = %d", value)
				rows, _ := DB2.Query(querySql)
				var childId int
				for rows.Next() {
					rows.Scan(&childId)
					channelIds = append(channelIds, childId)
				}
			}
		}
		channelIds = append(channelIds, channelId)
	}

	return
}

func getGameDatas(startTime int64, endTime int64) (gameDatas []GameData) {
	querySql := fmt.Sprintf(`SELECT gs.game_id,g.game_name,COUNT(gs.game_server_id) open_server_count 
	FROM gc_game_server as gs LEFT JOIN gc_game AS g on gs.game_id = g.game_id 
	WHERE gs.open_time BETWEEN %d AND %d GROUP BY gs.game_id`, startTime, endTime)

	rows, err := DB3.Query(querySql)

	if err != nil {
		fmt.Println(err)
		return
	}

	gameData := new(GameData)
	for rows.Next() {
		rows.Scan(&gameData.GameId, &gameData.GameName, &gameData.OpenServerCount)
		gameDatas = append(gameDatas, *gameData)
	}

	defer func() {
		rows.Close()
	}()

	return
}

func openDB() (DB *sql.DB) {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s", USERNAME, PASSWORD, NETWORK, SERVER, PORT, DATABASE)
	DB, err := sql.Open("mysql", dsn)

	if err != nil {
		panic(fmt.Sprintf("Open mysql failed,Error:%v\n", err))
	}

	DB.SetConnMaxLifetime(100 * time.Second) //最大连接周期，超过时间的连接就close
	DB.SetMaxOpenConns(100)                  //设置最大连接数
	DB.SetMaxIdleConns(16)                   //设置闲置连接数

	return
}

func openDB2() (DB2 *sql.DB) {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s", USERNAME2, PASSWORD2, NETWORK2, SERVER2, PORT2, DATABASE2)
	DB2, err := sql.Open("mysql", dsn)

	if err != nil {
		panic(fmt.Sprintf("Open mysql failed,Error:%v\n", err))
	}

	DB2.SetConnMaxLifetime(100 * time.Second) //最大连接周期，超过时间的连接就close
	DB2.SetMaxOpenConns(100)                  //设置最大连接数
	DB2.SetMaxIdleConns(16)                   //设置闲置连接数

	return
}

func openDB3() (DB3 *sql.DB) {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s", USERNAME3, PASSWORD3, NETWORK3, SERVER3, PORT3, DATABASE3)
	DB3, err := sql.Open("mysql", dsn)

	if err != nil {
		panic(fmt.Sprintf("Open mysql failed,Error:%v\n", err))
	}

	DB3.SetConnMaxLifetime(100 * time.Second) //最大连接周期，超过时间的连接就close
	DB3.SetMaxOpenConns(100)                  //设置最大连接数
	DB3.SetMaxIdleConns(16)                   //设置闲置连接数

	return
}

func openDB4() (DB4 *sql.DB) {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s", USERNAME4, PASSWORD4, NETWORK4, SERVER4, PORT4, DATABASE4)
	DB4, err := sql.Open("mysql", dsn)

	if err != nil {
		panic(fmt.Sprintf("Open mysql failed,Error:%v\n", err))
	}

	DB4.SetConnMaxLifetime(100 * time.Second) //最大连接周期，超过时间的连接就close
	DB4.SetMaxOpenConns(100)                  //设置最大连接数
	DB4.SetMaxIdleConns(16)                   //设置闲置连接数

	return
}

func resolveSecond(second int64) (time string) {

	minute := second / 60

	hour := minute / 60

	minute = minute % 60

	second = second - hour*3600 - minute*60

	time = fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)

	//fmt.Println(time)

	return
}
