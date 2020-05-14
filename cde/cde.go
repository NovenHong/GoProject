package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/techoner/gophp/serialize"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type ChannelId struct {
	ChannelId string `json:"channel_id"`
}

type ChannelData struct {
	ChannelId int64
	RoleIds   []string
}

type ChannelMonthCountData struct {
	ChannelId          int64 `db:"channel_id"`
	EffectiveNum       int   `db:"effective_num"`
	EffectiveNum130149 int   `db:"effective_num130_149"`
}

type ChannelUserLoginData struct {
	ChannelId                  int64 `db:"channel_id"`
	SameUniqueFlagUserCount    int   `db:"same_unique_flag_user_count"`
	SameIpUserCount            int   `db:"same_ip_user_count"`
	OneLoginEffectiveUserCount int   `db:"one_login_effective_user_count"`
}

type ChannelUserOnlineData struct {
	ChannelId     int64
	DateTime      int64
	Date          string
	UserId        int64
	RoleId        string
	OnlineTime    int
	LoginDayCount int
	AveOnlineTime int
}

type ChannelDimensionData struct {
	ChannelId                  int64
	DateTime                   int64
	Date                       string
	EffectiveNum               int
	EffectiveNum130149         int
	SameUniqueFlagUserCount    int
	SameIpUserCount            int
	OneLoginEffectiveUserCount int
	SilverCount                int
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
	PASSWORD2 string = "game123456"
	NETWORK2  string = "tcp"
	SERVER2   string = "120.132.17.131"
	PORT2     int    = 3306
	DATABASE2 string = "cj655"
)

var (
	USERNAME3 string = "cj655"
	PASSWORD3 string = "game123456"
	NETWORK3  string = "tcp"
	SERVER3   string = "120.132.31.31"
	PORT3     int    = 3306
	DATABASE3 string = "cj655"
)

var DB *sql.DB
var DB2 *sql.DB
var DB3 *sql.DB
var loc *time.Location
var channelId int64
var months []string

var totalCount int64
var totalSuccessCount int64
var totalErrorCount int64

var taskCount int64
var taskSuccessCount int64
var taskErrorCount int64

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

	loc, _ = time.LoadLocation("Local")

	//parse flag
	flag.Int64Var(&channelId, "channel-id", 0, "单个渠道计算")
	var month string
	flag.StringVar(&month, "month", "", "当前的月份")
	flag.Parse()

	if month != "" {
		months = strings.Split(month, ",")
	} else {
		months = append(months, time.Now().Format("2006-01"))
	}
}

func main() {

	allStartTime := time.Now().Unix()

	for _, month := range months {

		monthDays := getFirstLastMonthDay(month)
		startDate := monthDays["firstDay"]
		endDate := monthDays["lastDay"]
		startTime := dateToUnixTime(startDate)
		endTime := dateToUnixTime(endDate) + 86399

		fmt.Println(fmt.Sprintf("Task:%s begin StartDate:%s EndDate:%s RunDate:%s", month, startDate, endDate, time.Now().Format("2006-01-02 15:04:05")))
		startTask(startTime, endTime)
	}

	allEndTime := time.Now().Unix()

	fmt.Println(fmt.Sprintf("All task is compeleted,SuccessRow:%d ErrorRow:%d TotalRow:%d Time:%s",
		totalSuccessCount, totalErrorCount, totalCount, resolveSecond(allEndTime-allStartTime)))
}

func startTask(startTime int64, endTime int64) {
	taskStartTime := time.Now().Unix()
	taskCount = 0
	taskSuccessCount = 0
	taskErrorCount = 0

	channelDatas := getChannelDatas(startTime, endTime)

	//fmt.Println(channelDatas)

	for _, channelData := range channelDatas {
		channelMonthCountData := getChannelMonthCountData(channelData.ChannelId, startTime, endTime)

		channelUserLoginData := getChannelUserLoginData(channelData.ChannelId, startTime, endTime)

		//银两副本数据
		silverCount := getSilverCount(channelData.RoleIds, startTime, endTime)

		//登录时长用户数据
		channelUserOnlineDatas := getChannelUserOnlineDatas(channelData, startTime, endTime)
		for _, channelUserOnlineData := range channelUserOnlineDatas {
			saveChannelUserOnlineData(channelUserOnlineData)
		}

		channelDimensionData := new(ChannelDimensionData)
		channelDimensionData.ChannelId = channelData.ChannelId
		channelDimensionData.DateTime = startTime
		channelDimensionData.Date = unixTimeToDate(startTime)
		channelDimensionData.EffectiveNum = channelMonthCountData.EffectiveNum
		channelDimensionData.EffectiveNum130149 = channelMonthCountData.EffectiveNum130149
		channelDimensionData.SameUniqueFlagUserCount = channelUserLoginData.SameUniqueFlagUserCount
		channelDimensionData.SameIpUserCount = channelUserLoginData.SameIpUserCount
		channelDimensionData.OneLoginEffectiveUserCount = channelUserLoginData.OneLoginEffectiveUserCount
		channelDimensionData.SilverCount = silverCount

		saveChannelDimensionData(*channelDimensionData)
	}

	taskEndTime := time.Now().Unix()

	fmt.Println(fmt.Sprintf("Task:%s is compeleted,SuccessRow:%d ErrorRow:%d TotalRow:%d Time:%s",
		unixTimeToDate(startTime), taskSuccessCount, taskErrorCount, taskCount, resolveSecond(taskEndTime-taskStartTime)))
}

func isExistChannelDimensionData(channelDimensionData ChannelDimensionData) (id int64) {
	querySql := fmt.Sprintf(`SELECT id 
	FROM gc_channel_dimension_data WHERE channel_id = %d AND date_time = %d LIMIT 1`,
		channelDimensionData.ChannelId, channelDimensionData.DateTime)

	row := DB.QueryRow(querySql)
	row.Scan(&id)

	return
}

func saveChannelDimensionData(channelDimensionData ChannelDimensionData) {

	var err error

	if id := isExistChannelDimensionData(channelDimensionData); id == 0 {
		_, err = DB.Exec(
			`insert INTO gc_channel_dimension_data(
				channel_id,date_time,date,effective_num_130_149,effective_num,same_unique_flag_user_count,same_ip_user_count,one_login_effective_user_count,silver_count) 
				values(?,?,?,?,?,?,?,?,?)`,
			channelDimensionData.ChannelId,
			channelDimensionData.DateTime,
			channelDimensionData.Date,
			channelDimensionData.EffectiveNum130149,
			channelDimensionData.EffectiveNum,
			channelDimensionData.SameUniqueFlagUserCount,
			channelDimensionData.SameIpUserCount,
			channelDimensionData.OneLoginEffectiveUserCount,
			channelDimensionData.SilverCount)
	} else {
		_, err = DB.Exec(`UPDATE gc_channel_dimension_data SET 
		effective_num_130_149 = ?,effective_num = ?,same_unique_flag_user_count = ?,
		same_ip_user_count = ?,one_login_effective_user_count = ?,silver_count = ? WHERE id=?`,
			channelDimensionData.EffectiveNum130149,
			channelDimensionData.EffectiveNum,
			channelDimensionData.SameUniqueFlagUserCount,
			channelDimensionData.SameIpUserCount,
			channelDimensionData.OneLoginEffectiveUserCount,
			channelDimensionData.SilverCount,
			id)
	}

	if err != nil {
		totalErrorCount++
		taskErrorCount++
		fmt.Println(err)
	} else {
		totalSuccessCount++
		taskSuccessCount++
	}

	totalCount++
	taskCount++
}

func isExistChannelUserOnlineData(channelUserOnlineData ChannelUserOnlineData) (id int64) {
	querySql := fmt.Sprintf(`SELECT id 
	FROM gc_channel_user_online WHERE channel_id = %d AND role_id = '%s' AND date_time = %d LIMIT 1`,
		channelUserOnlineData.ChannelId, channelUserOnlineData.RoleId, channelUserOnlineData.DateTime)

	row := DB.QueryRow(querySql)
	row.Scan(&id)

	return
}

func saveChannelUserOnlineData(channelUserOnlineData ChannelUserOnlineData) {
	if id := isExistChannelUserOnlineData(channelUserOnlineData); id == 0 {
		DB.Exec(
			"insert INTO gc_channel_user_online(channel_id,date_time,date,user_id,role_id,online_time,login_day_count,ave_online_time) values(?,?,?,?,?,?,?,?)",
			channelUserOnlineData.ChannelId,
			channelUserOnlineData.DateTime,
			channelUserOnlineData.Date,
			channelUserOnlineData.UserId,
			channelUserOnlineData.RoleId,
			channelUserOnlineData.OnlineTime,
			channelUserOnlineData.LoginDayCount,
			channelUserOnlineData.AveOnlineTime)
	} else {
		DB.Exec(
			"UPDATE gc_channel_user_online SET online_time = ?,login_day_count = ?,ave_online_time = ? WHERE id=?",
			channelUserOnlineData.OnlineTime,
			channelUserOnlineData.LoginDayCount,
			channelUserOnlineData.AveOnlineTime,
			id)
	}
}

func getChannelUserOnlineDatas(channelData ChannelData, startTime int64, endTime int64) (channelUserOnlineDatas []ChannelUserOnlineData) {
	for _, roleId := range channelData.RoleIds {
		//登录天数
		querySql := fmt.Sprintf(`SELECT count(distinct FROM_UNIXTIME(time,'%%Y-%%m-%%d')) login_day_count FROM gc_cp_login 
		WHERE user_id = '%s' AND (time BETWEEN %d AND %d)`, roleId, startTime, endTime)

		var loginDayCount int
		row := DB2.QueryRow(querySql)
		row.Scan(&loginDayCount)

		//在线时长
		querySql2 := fmt.Sprintf(`SELECT sum(online_time) total_online_time FROM gc_cp_logout 
		WHERE user_id = '%s' AND (time BETWEEN %d AND %d)`, roleId, startTime, endTime)

		var totalOnlineTime int
		row2 := DB2.QueryRow(querySql2)
		row2.Scan(&totalOnlineTime)

		//用户id
		querySql3 := fmt.Sprintf(`SELECT u.user_id FROM gc_user as u LEFT JOIN gc_user_role as ur ON u.username = ur.username WHERE ur.role_id = '%s'`, roleId)

		var userId int64
		row3 := DB3.QueryRow(querySql3)
		row3.Scan(&userId)

		channelUserOnlineData := new(ChannelUserOnlineData)

		channelUserOnlineData.ChannelId = channelData.ChannelId
		channelUserOnlineData.DateTime = startTime
		channelUserOnlineData.Date = unixTimeToDate(startTime)
		channelUserOnlineData.UserId = userId
		channelUserOnlineData.RoleId = roleId
		channelUserOnlineData.OnlineTime = totalOnlineTime
		channelUserOnlineData.LoginDayCount = loginDayCount
		if loginDayCount > 0 {
			channelUserOnlineData.AveOnlineTime = totalOnlineTime / loginDayCount
		}

		//fmt.Println(channelUserOnlineData)
		//os.Exit(0)

		channelUserOnlineDatas = append(channelUserOnlineDatas, *channelUserOnlineData)
	}

	return
}

func getSilverCount(roleIds []string, startTime int64, endTime int64) (silverCount int) {
	querySql := fmt.Sprintf(`SELECT count(distinct user_id) silver_count FROM gc_cp_silver WHERE (user_id in ('%s')) AND (time BETWEEN %d AND %d)`,
		strings.Join(roleIds, "','"), startTime, endTime)

	//fmt.Println(querySql)

	row := DB2.QueryRow(querySql)
	row.Scan(&silverCount)

	return
}

func getChannelUserLoginData(channelId int64, startTime int64, endTime int64) (channelUserLoginData ChannelUserLoginData) {
	querySql := fmt.Sprintf(`SELECT channel_id,same_unique_flag_user_count,same_ip_user_count,one_login_effective_user_count FROM gc_channel_user_login 
	WHERE (channel_id = %d) AND (date_time BETWEEN %d AND %d) limit 1`, channelId, startTime, endTime)

	row := DB.QueryRow(querySql)
	row.Scan(&channelUserLoginData.ChannelId, &channelUserLoginData.SameUniqueFlagUserCount, &channelUserLoginData.SameIpUserCount, &channelUserLoginData.OneLoginEffectiveUserCount)

	return
}

func getChannelMonthCountData(channelId int64, startTime int64, endTime int64) (channelMonthCountData ChannelMonthCountData) {
	querySql := fmt.Sprintf(`SELECT channel_id,effective_num,effective_num130_149 FROM gc_channel_month_count 
	WHERE (channel_id = %d) AND (date_time BETWEEN %d AND %d) limit 1`, channelId, startTime, endTime)

	row := DB.QueryRow(querySql)
	row.Scan(&channelMonthCountData.ChannelId, &channelMonthCountData.EffectiveNum, &channelMonthCountData.EffectiveNum130149)

	return
}

func getChannelDatas(startTime int64, endTime int64) (channelDatas []ChannelData) {

	channelIds := getChannelIds()

	for _, channelId := range channelIds {

		channelData := new(ChannelData)
		channelData.ChannelId = int64(myAtoi(channelId.ChannelId))
		channelData.RoleIds = getRoleIds(channelData.ChannelId, startTime, endTime)
		channelDatas = append(channelDatas, *channelData)
	}

	return
}

func getRoleIds(channelId int64, startTime int64, endTime int64) (roleIds []string) {
	querySql := fmt.Sprintf(`SELECT ur.role_id FROM gc_user_role as ur LEFT JOIN gc_user as u ON ur.username = u.username 
	WHERE (ur.is_effective = 1) AND (u.channel_id = %d) AND (u.reg_time BETWEEN %d AND %d) AND (ur.dabiao_time BETWEEN %d AND %d)`,
		channelId, startTime, endTime, startTime, endTime)

	rows, err := DB3.Query(querySql)

	if err != nil {
		fmt.Println(err)
		return
	}

	var roleId string
	for rows.Next() {
		rows.Scan(&roleId)
		roleIds = append(roleIds, roleId)
	}

	defer func() {
		rows.Close()
	}()

	return
}

func getChannelIds() (channelIds []ChannelId) {
	where := "(status > 0) AND (channel_is_delete = 0) AND (channel_id > 0)"

	if channelId > 0 {
		where = fmt.Sprintf("channel_id = %d", channelId)
	}

	where2, _ := serialize.Marshal(where)

	where3 := string(where2)

	field := "channel_id"

	myUrl := "https://admin.cj655.com/api.php?m=channelpublic&a=channel_data&api_key=TbjoLfLhnikp92hyd8dx0ozCcEipII2Z"

	resp, err := http.PostForm(myUrl, url.Values{"where": {where3}, "field": {field}})

	if err != nil {
		fmt.Println(err)
		return
	}

	body, _ := ioutil.ReadAll(resp.Body)

	//fmt.Println(string(body))
	//os.Exit(0)

	_ = json.Unmarshal(body, &channelIds)

	return
}

func myAtoi(s string) (i int) {
	i, _ = strconv.Atoi(s)
	return
}

func dateToUnixTime(date string) (unixTime int64) {
	theTime, _ := time.ParseInLocation("2006-01-02", date, loc)
	unixTime = theTime.Unix()
	return
}

func unixTimeToDate(unixTime int64) (date string) {
	date = time.Unix(unixTime, 0).Format("2006-01")
	return
}

func getFirstLastMonthDay(monthDate string) (monthDays map[string]string) {
	theTime, _ := time.ParseInLocation("2006-01", monthDate, loc)
	year, month, _ := theTime.Date()

	firstMonthUTC := time.Date(year, month, 1, 0, 0, 0, 0, loc)
	firstMonthDay := firstMonthUTC.Format("2006-01-02")
	lastMonthDay := firstMonthUTC.AddDate(0, 1, -1).Format("2006-01-02")

	monthDays = map[string]string{}

	monthDays["firstDay"] = firstMonthDay
	monthDays["lastDay"] = lastMonthDay

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

func openDB2() (DB *sql.DB) {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s", USERNAME2, PASSWORD2, NETWORK2, SERVER2, PORT2, DATABASE2)
	DB, err := sql.Open("mysql", dsn)

	if err != nil {
		panic(fmt.Sprintf("Open mysql failed,Error:%v\n", err))
	}

	DB.SetConnMaxLifetime(100 * time.Second) //最大连接周期，超过时间的连接就close
	DB.SetMaxOpenConns(100)                  //设置最大连接数
	DB.SetMaxIdleConns(16)                   //设置闲置连接数

	return
}

func openDB3() (DB *sql.DB) {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s", USERNAME3, PASSWORD3, NETWORK3, SERVER3, PORT3, DATABASE3)
	DB, err := sql.Open("mysql", dsn)

	if err != nil {
		panic(fmt.Sprintf("Open mysql failed,Error:%v\n", err))
	}

	DB.SetConnMaxLifetime(100 * time.Second) //最大连接周期，超过时间的连接就close
	DB.SetMaxOpenConns(100)                  //设置最大连接数
	DB.SetMaxIdleConns(16)                   //设置闲置连接数

	return
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
