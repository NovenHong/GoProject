package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/techoner/gophp/serialize"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type ChannelId struct {
	ChannelId string `json:"channel_id"`
}

type ChannelData struct {
	ChannelId int64
	UserDatas []UserData
}

type UserData struct {
	ChannelId int64  `db:"channel_id"`
	UserId    int64  `db:"user_id"`
	Username  string `db:"username"`
}

type UserPlayDataDetail struct {
	UserId     int64          `db:"user_id"`
	Username   string         `db:"username"`
	UniqueFlag sql.NullString `db:"unique_flag"`
	Ip         sql.NullString `db:"ip"`
}

type UserPlayData struct {
	UserId          int64          `db:"user_id"`
	Username        sql.NullString `db:"username"`
	UniqueFlagCount int            `db:"unique_flag_count"`
}

type OneLoginUserData struct {
	UserId int64 `db:"user_id"`
}

type OneLoginEffectiveUserCountData struct {
	Count string `json:"one_login_effective_user_count"`
}

type ChannelUserLoginData struct {
	Id                         int64
	ChannelId                  int64
	SameUniqueFlagUserCount    int
	SameIpUserCount            int
	OneLoginUserCount          int
	OneLoginEffectiveUserCount int
	Date                       string
	DateTime                   int64
}

type ChannelSameUniqueFlagUserData struct {
	ChannelId       int64
	UserId          int64
	Username        string
	UniqueFlag      string
	UserUniqueFlags []string
	Date            string
	DateTime        int64
}

type UniqueFlagData struct {
	UniqueFlag                     string
	Count                          int
	ChannelSameUniqueFlagUserDatas []ChannelSameUniqueFlagUserData
}

type ChannelSameIpUserData struct {
	ChannelId int64
	UserId    int64
	Username  string
	Ip        string
	UserIps   []string
	Date      string
	DateTime  int64
}

type IpData struct {
	Ip                     string
	Count                  int
	ChannelSameIpUserDatas []ChannelSameIpUserData
}

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

var logDirPath string
var failureLogger *log.Logger

var channelDatas []ChannelData

var startTimeDate string
var endTimeDate string
var startTime int64
var endTime int64
var loc *time.Location
var quarySql string
var date string
var dateTime int64
var limit int
var months []string
var update int
var channelId int64

var totalCount int64 = 0
var totalSuccessCount int64 = 0
var totalErrorCount int64 = 0

var taskCount int64 = 0
var taskSuccessCount int64 = 0
var taskErrorCount int64 = 0

var allStartTime int64
var allEndTime int64

var taskStartTime int64
var taskEndTime int64

func init() {
	myOS := os.Getenv("OS")
	if myOS == "Windows_NT" {
		PASSWORD = ""
	} else {
		PASSWORD = "game123456"
	}

	DB = openDB()

	loc, _ = time.LoadLocation("Local")

	months = []string{}

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

	initFlag()
}

func main() {

	allStartTime = time.Now().Unix()

	for _, month := range months {

		monthDays := getFirstLastMonthDay(month)
		startTimeDate = monthDays["firstDay"]
		endTimeDate = monthDays["lastDay"]

		initTime()

		initLog()

		startTask()

		time.Sleep(time.Second * 5)

		endLog()
	}

	allEndTime = time.Now().Unix()

	fmt.Println(fmt.Sprintf("All task is compeleted,SuccessRow:%d ErrorRow:%d TotalRow:%d Time:%s",
		totalSuccessCount, totalErrorCount, totalCount, resolveSecond(allEndTime-allStartTime)))
}

func startTask() {

	//fmt.Println(time.Unix(startTime,0).Format("2006-01-02 15:04:05"))
	//fmt.Println(time.Unix(endTime,0).Format("2006-01-02 15:04:05"))
	fmt.Println(fmt.Sprintf("Task:%s begin StartDate:%s EndDate:%s RunDate:%s", date, startTimeDate, endTimeDate, time.Now().Format("2006-01-02 15:04:05")))

	taskStartTime := time.Now().Unix()

	taskCount = 0
	taskSuccessCount = 0
	taskErrorCount = 0

	channelDatas = getChannelDatas()

	if len(channelDatas) == 0 {
		panic("ChannelDatas is empty")
	}

	for _, channelData := range channelDatas {

		//fmt.Println(channelData.ChannelId)

		userIds := getUserIds(channelData.UserDatas)

		if len(userIds) == 0 {
			//failureLogger.Output(0, fmt.Sprintf("ChannelId:%d Error:UserData is empty", channelData.ChannelId))
			continue
		}

		userDataDetails := getUserDataDetails(userIds)

		uniqueFlagDatas5 := getSameUniqueFlagUserCountAndUser(userDataDetails, channelData.UserDatas)

		for _, uniqueFlagData := range uniqueFlagDatas5 {
			for _, channelSameUniqueFlagUserData := range uniqueFlagData.ChannelSameUniqueFlagUserDatas {
				saveChannelSameUniqueFlagUserData(channelSameUniqueFlagUserData)
			}
		}

		ipDatas10 := getSameIpUserCountAndUser(userDataDetails, channelData.UserDatas)

		for _, ipData := range ipDatas10 {
			for _, channelSameIpUserData := range ipData.ChannelSameIpUserDatas {
				saveChannelIpUserData(channelSameIpUserData)
			}
		}

		//time.Sleep(time.Second*1)

		//fmt.Println(len(sameUniqueFlagUsers))

		userPlayDatas := getUserPlayDatas(userIds)

		oneLoginUsernames := getOneLoginUsernames(userPlayDatas)

		oneLoginUserCount := len(oneLoginUsernames)

		oneLoginEffectiveUserCount := getOneLoginEffectiveUserCount(oneLoginUsernames)

		channelUserLoginData := new(ChannelUserLoginData)
		channelUserLoginData.ChannelId = channelData.ChannelId
		channelUserLoginData.SameUniqueFlagUserCount = len(uniqueFlagDatas5)
		channelUserLoginData.SameIpUserCount = len(ipDatas10)
		channelUserLoginData.OneLoginUserCount = oneLoginUserCount
		channelUserLoginData.OneLoginEffectiveUserCount = oneLoginEffectiveUserCount
		channelUserLoginData.Date = date
		channelUserLoginData.DateTime = dateTime

		saveChannelUserLoginData(*channelUserLoginData)

		//time.Sleep(time.Second*1)
	}

	taskEndTime := time.Now().Unix()

	fmt.Println(fmt.Sprintf("Task:%s is compeleted,SuccessRow:%d ErrorRow:%d TotalRow:%d Time:%s",
		date, taskSuccessCount, taskErrorCount, taskCount, resolveSecond(taskEndTime-taskStartTime)))

}

func getOneLoginUsernames(userPlayDatas []UserPlayData) (oneLoginUsernames []string) {
	for _, userPlayData := range userPlayDatas {
		if userPlayData.UniqueFlagCount == 1 && userPlayData.Username.Valid {
			oneLoginUsernames = append(oneLoginUsernames, userPlayData.Username.String)
		}
	}
	return
}

func getOneLoginEffectiveUserCount(oneLoginUsernames []string) (oneLoginEffectiveUserCount int) {
	//oneLoginUsernames = append(oneLoginUsernames,"ozPSJ1CHpPCRUOmLNB94eQcdiUj4")
	if len(oneLoginUsernames) == 0 {
		return
	}

	where := fmt.Sprintf("( username in ( '%s' ) ) AND is_effective = 1 AND dabiao_time BETWEEN %d AND %d", strings.Join(oneLoginUsernames, "','"), startTime, endTime)

	where2, _ := serialize.Marshal(where)

	where3 := string(where2)

	field := "count(username) one_login_effective_user_count"

	resp, err := http.PostForm("http://dj.cj655.com/api.php?m=player&a=admin_role_array7", url.Values{"where": {where3}, "field": {field}})

	if err != nil {
		failureLogger.Output(0, fmt.Sprintf("Error:%v", err))
		//failureLogger.Output(0,fmt.Sprintf("Error:%v",err))
		return
	}

	body, _ := ioutil.ReadAll(resp.Body)

	oneLoginEffectiveUserCountDatas := []OneLoginEffectiveUserCountData{}

	_ = json.Unmarshal(body, &oneLoginEffectiveUserCountDatas)

	oneLoginEffectiveUserCount = myAtoi(oneLoginEffectiveUserCountDatas[0].Count)

	return

}

func getSameIpUserCountAndUser(userDataDetails []UserPlayDataDetail, userDatas []UserData) (ipDatas10 []IpData) {
	if len(userDataDetails) == 0 {
		return
	}

	channelSameIpUserDatas := []ChannelSameIpUserData{}

	for _, userData := range userDatas {
		userIps := []string{}
		for _, userDataDetail := range userDataDetails {
			if userData.UserId == userDataDetail.UserId && userDataDetail.Ip.Valid {
				userIps = append(userIps, userDataDetail.Ip.String)
			}
		}

		channelSameIpUserData := new(ChannelSameIpUserData)
		channelSameIpUserData.ChannelId = userData.ChannelId
		channelSameIpUserData.UserId = userData.UserId
		channelSameIpUserData.Username = userData.Username
		channelSameIpUserData.UserIps = userIps
		channelSameIpUserData.Date = date
		channelSameIpUserData.DateTime = dateTime

		//fmt.Println(channelSameIpUserData)

		channelSameIpUserDatas = append(channelSameIpUserDatas, *channelSameIpUserData)
	}

	ipDatas := map[string]IpData{}

	for index, channelSameIpUserData := range channelSameIpUserDatas {
		for _, ip := range channelSameIpUserData.UserIps {
			ipData, ok := ipDatas[ip]
			if !ok {
				ipData = *new(IpData)
				ipData.Ip = ip
			}

			for index2, channelSameIpUserData2 := range channelSameIpUserDatas {
				if index == index2 {
					continue
				}
				if inArray(ip, channelSameIpUserData2.UserIps) {
					isNotExist := true
					for _, v := range ipData.ChannelSameIpUserDatas {
						if channelSameIpUserData2.UserId == v.UserId {
							isNotExist = false
							break
						}
					}
					if isNotExist {
						ipData.Count++
						channelSameIpUserData2.Ip = ip
						ipData.ChannelSameIpUserDatas = append(ipData.ChannelSameIpUserDatas, channelSameIpUserData2)
					}
				}
			}

			ipDatas[ip] = ipData
		}
	}

	for _, ipData := range ipDatas {
		if ipData.Count >= 10 {
			ipDatas10 = append(ipDatas10, ipData)
		}
	}

	return
}

func getSameUniqueFlagUserCountAndUser(userDataDetails []UserPlayDataDetail, userDatas []UserData) (uniqueFlagDatas5 []UniqueFlagData) {

	if len(userDataDetails) == 0 {
		return
	}

	channelSameUniqueFlagUserDatas := []ChannelSameUniqueFlagUserData{}

	for _, userData := range userDatas {
		userUniqueFlags := []string{}
		for _, userDataDetail := range userDataDetails {
			if userData.UserId == userDataDetail.UserId && userDataDetail.UniqueFlag.Valid {
				userUniqueFlags = append(userUniqueFlags, userDataDetail.UniqueFlag.String)
			}
		}

		channelSameUniqueFlagUserData := new(ChannelSameUniqueFlagUserData)
		channelSameUniqueFlagUserData.ChannelId = userData.ChannelId
		channelSameUniqueFlagUserData.UserId = userData.UserId
		channelSameUniqueFlagUserData.Username = userData.Username
		channelSameUniqueFlagUserData.UserUniqueFlags = userUniqueFlags
		channelSameUniqueFlagUserData.Date = date
		channelSameUniqueFlagUserData.DateTime = dateTime

		channelSameUniqueFlagUserDatas = append(channelSameUniqueFlagUserDatas, *channelSameUniqueFlagUserData)
		//fmt.Println(userUniqueFlags)
	}

	uniqueFlagDatas := map[string]UniqueFlagData{}

	for index, channelSameUniqueFlagUserData := range channelSameUniqueFlagUserDatas {
		for _, uniqueFlag := range channelSameUniqueFlagUserData.UserUniqueFlags {

			uniqueFlagData, ok := uniqueFlagDatas[uniqueFlag]
			if !ok {
				uniqueFlagData = *new(UniqueFlagData)
				uniqueFlagData.UniqueFlag = uniqueFlag
			}

			for index2, channelSameUniqueFlagUserData2 := range channelSameUniqueFlagUserDatas {
				if index == index2 {
					continue
				}
				if inArray(uniqueFlag, channelSameUniqueFlagUserData2.UserUniqueFlags) {
					isNotExist := true
					for _, v := range uniqueFlagData.ChannelSameUniqueFlagUserDatas {
						if channelSameUniqueFlagUserData2.UserId == v.UserId {
							isNotExist = false
							break
						}
					}
					if isNotExist {
						uniqueFlagData.Count++
						channelSameUniqueFlagUserData2.UniqueFlag = uniqueFlag
						uniqueFlagData.ChannelSameUniqueFlagUserDatas = append(uniqueFlagData.ChannelSameUniqueFlagUserDatas, channelSameUniqueFlagUserData2)
					}
				}
			}

			uniqueFlagDatas[uniqueFlag] = uniqueFlagData
		}
	}

	for _, uniqueFlagData := range uniqueFlagDatas {
		if uniqueFlagData.Count >= 5 {
			uniqueFlagDatas5 = append(uniqueFlagDatas5, uniqueFlagData)
		}
	}

	//os.Exit(0)

	return

}

func getUserPlayDatas(userIds []string) (userPlayDatas []UserPlayData) {

	if len(userIds) == 0 {
		return
	}

	quarySql = fmt.Sprintf(`SELECT user_id,username,count(unique_flag) unique_flag_count 
	FROM gc_user_play_data WHERE ( user_id in ( %s ) ) AND ( ( login_time BETWEEN %d AND %d ) ) GROUP BY user_id`,
		strings.Join(userIds, ","), startTime, endTime)

	rows, err := DB.Query(quarySql)

	if err != nil {
		failureLogger.Output(0, fmt.Sprintf("Sql:%s Error:%v", quarySql, err))
		return
	}

	userPlayData := new(UserPlayData)

	userPlayDatas = []UserPlayData{}

	for rows.Next() {
		rows.Scan(&userPlayData.UserId, &userPlayData.Username, &userPlayData.UniqueFlagCount)

		userPlayDatas = append(userPlayDatas, *userPlayData)
	}

	defer func() {
		rows.Close()
	}()

	return
}

func getUserDataDetails(userIds []string) (userDataDetails []UserPlayDataDetail) {

	if len(userIds) == 0 {
		return
	}

	quarySql = fmt.Sprintf(`SELECT unique_flag,ip,user_id,username 
	FROM gc_user_play_data 
	WHERE ( user_id in ( %s ) ) AND ( ( login_time BETWEEN %d AND %d ) )`, strings.Join(userIds, ","), startTime, endTime)

	rows, err := DB.Query(quarySql)

	if err != nil {
		failureLogger.Output(0, fmt.Sprintf("Sql:%s Error:%v", quarySql, err))
		return
	}

	userPlayDataDetail := new(UserPlayDataDetail)

	userDataDetails = []UserPlayDataDetail{}

	for rows.Next() {
		rows.Scan(&userPlayDataDetail.UniqueFlag, &userPlayDataDetail.Ip, &userPlayDataDetail.UserId, &userPlayDataDetail.Username)
		//fmt.Println(userPlayDataDetail)
		userDataDetails = append(userDataDetails, *userPlayDataDetail)
	}

	defer func() {
		rows.Close()
	}()

	return
}

func getUserIds(userDatas []UserData) (userIds []string) {

	userIds = []string{}
	for _, value := range userDatas {
		userIds = append(userIds, strconv.FormatInt(value.UserId, 10))
	}

	return
}

func getUserDatas(channelId int64) (userDatas []UserData) {
	quarySql = fmt.Sprintf(`SELECT user_id,channel_id,username FROM gc_user 
	WHERE (channel_id = %d) AND (reg_time BETWEEN %d AND %d) `, channelId, startTime, endTime)

	//fmt.Println(quarySql)
	//os.Exit(0)

	rows, err := DB.Query(quarySql)

	if err != nil {
		failureLogger.Output(0, fmt.Sprintf("Sql:%s Error:%v", quarySql, err))
		return
	}

	userData := new(UserData)

	for rows.Next() {
		rows.Scan(&userData.UserId, &userData.ChannelId, &userData.Username)
		userDatas = append(userDatas, *userData)
	}

	defer func() {
		rows.Close()
	}()

	return
}

func getChannelDatas() (channelDatas []ChannelData) {

	channelIds := getChannelIds()

	for _, channelId := range channelIds {

		channelData := new(ChannelData)
		userDatas := getUserDatas(int64(myAtoi(channelId.ChannelId)))
		channelData.ChannelId = int64(myAtoi(channelId.ChannelId))
		channelData.UserDatas = userDatas
		channelDatas = append(channelDatas, *channelData)
	}

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

	myUrl := "https://www.cj655.com/api.php?m=channelpublic&a=channel_data&api_key=TbjoLfLhnikp92hyd8dx0ozCcEipII2Z"

	if limit > 0 {
		myUrl = fmt.Sprintf("https://www.cj655.com/api.php?m=channelpublic&a=channel_data&limit=%d&api_key=TbjoLfLhnikp92hyd8dx0ozCcEipII2Z", limit)
	}

	resp, err := http.PostForm(myUrl, url.Values{"where": {where3}, "field": {field}})

	if err != nil {
		failureLogger.Output(0, fmt.Sprintf("Error:%v", err))
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

func openDB() (DB *sql.DB) {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s", USERNAME, PASSWORD, NETWORK, SERVER, PORT, DATABASE)
	DB, err = sql.Open("mysql", dsn)

	if err != nil {
		panic(fmt.Sprintf("Open mysql failed,Error:%v\n", err))
	}

	DB.SetConnMaxLifetime(100 * time.Second) //最大连接周期，超过时间的连接就close
	DB.SetMaxOpenConns(100)                  //设置最大连接数
	DB.SetMaxIdleConns(16)                   //设置闲置连接数

	return
}

func initFlag() {
	var currentMonth string
	flag.StringVar(&currentMonth, "month", "", "当前的月份")
	flag.IntVar(&limit, "limit", 0, "全部条数")
	flag.IntVar(&update, "update", 1, "是否更新主表")
	flag.Int64Var(&channelId, "channelId", 0, "单个渠道计算")
	flag.Parse()

	if currentMonth != "" {
		months = strings.Split(currentMonth, ",")
	}
}

func initTime() {
	//startTime :=  time.Now().AddDate(0, -2, 0).Format("2006-01-02 15:04:05")
	theTime, _ := time.ParseInLocation("2006-01-02", startTimeDate, loc)
	startTime = theTime.Unix()

	//endTimeDate := time.Now().Format("2006-01-02")
	theTime, _ = time.ParseInLocation("2006-01-02", endTimeDate, loc)
	endTime = theTime.Unix() + 86399

	date = theTime.Format("2006-01")

	theTime2, _ := time.ParseInLocation("2006-01", date, loc)

	dateTime = theTime2.Unix()
}

func isExistChannelUserLoginData(channelUserLoginData ChannelUserLoginData) (id int64) {
	quarySql2 := fmt.Sprintf(`Select id 
	FROM gc_channel_user_login WHERE channel_id = %d AND date = '%s' LIMIT 1`,
		channelUserLoginData.ChannelId,
		channelUserLoginData.Date,
	)

	row := DB.QueryRow(quarySql2)
	row.Scan(&id)

	return
}

func isExistChannelSameUniqueFlagUserData(channelSameUniqueFlagUserData ChannelSameUniqueFlagUserData) (id int64) {
	quarySql2 := fmt.Sprintf(`Select id 
	FROM gc_channel_same_unique_flag_user WHERE channel_id = %d AND user_id = %d AND unique_flag = '%s' AND date = '%s' LIMIT 1`,
		channelSameUniqueFlagUserData.ChannelId,
		channelSameUniqueFlagUserData.UserId,
		channelSameUniqueFlagUserData.UniqueFlag,
		channelSameUniqueFlagUserData.Date,
	)

	row := DB.QueryRow(quarySql2)
	row.Scan(&id)

	return
}

func isExistChannelSameIpUserData(channelSameIpUserData ChannelSameIpUserData) (id int64) {
	quarySql2 := fmt.Sprintf(`Select id 
	FROM gc_channel_same_ip_user WHERE channel_id = %d AND user_id = %d AND ip = '%s' AND date = '%s' LIMIT 1`,
		channelSameIpUserData.ChannelId,
		channelSameIpUserData.UserId,
		channelSameIpUserData.Ip,
		channelSameIpUserData.Date,
	)

	row := DB.QueryRow(quarySql2)
	row.Scan(&id)

	return
}

func saveChannelSameUniqueFlagUserData(channelSameUniqueFlagUserData ChannelSameUniqueFlagUserData) {

	if id := isExistChannelSameUniqueFlagUserData(channelSameUniqueFlagUserData); id == 0 {
		DB.Exec(
			"insert INTO gc_channel_same_unique_flag_user(channel_id,user_id,username,unique_flag,date,date_time) values(?,?,?,?,?,?)",
			channelSameUniqueFlagUserData.ChannelId,
			channelSameUniqueFlagUserData.UserId,
			channelSameUniqueFlagUserData.Username,
			channelSameUniqueFlagUserData.UniqueFlag,
			channelSameUniqueFlagUserData.Date,
			channelSameUniqueFlagUserData.DateTime,
		)
	}
}

func saveChannelIpUserData(channelSameIpUserData ChannelSameIpUserData) {

	if id := isExistChannelSameIpUserData(channelSameIpUserData); id == 0 {
		DB.Exec(
			"insert INTO gc_channel_same_ip_user(channel_id,user_id,username,ip,date,date_time) values(?,?,?,?,?,?)",
			channelSameIpUserData.ChannelId,
			channelSameIpUserData.UserId,
			channelSameIpUserData.Username,
			channelSameIpUserData.Ip,
			channelSameIpUserData.Date,
			channelSameIpUserData.DateTime,
		)
	}

}

func saveChannelUserLoginData(channelUserLoginData ChannelUserLoginData) {

	if channelUserLoginData.SameUniqueFlagUserCount == 0 &&
		channelUserLoginData.SameIpUserCount == 0 &&
		channelUserLoginData.OneLoginUserCount == 0 &&
		channelUserLoginData.OneLoginEffectiveUserCount == 0 {
		return
	}

	var err error

	if id := isExistChannelUserLoginData(channelUserLoginData); id > 0 {
		_, err = DB.Exec(
			"UPDATE gc_channel_user_login SET same_unique_flag_user_count = ?,same_ip_user_count = ?,one_login_user_count = ?,one_login_effective_user_count=? WHERE id=?",
			channelUserLoginData.SameUniqueFlagUserCount,
			channelUserLoginData.SameIpUserCount,
			channelUserLoginData.OneLoginUserCount,
			channelUserLoginData.OneLoginEffectiveUserCount,
			id,
		)
	} else {
		_, err = DB.Exec(
			"insert INTO gc_channel_user_login(channel_id,same_unique_flag_user_count,same_ip_user_count,one_login_user_count,one_login_effective_user_count,date,date_time) values(?,?,?,?,?,?,?)",
			channelUserLoginData.ChannelId,
			channelUserLoginData.SameUniqueFlagUserCount,
			channelUserLoginData.SameIpUserCount,
			channelUserLoginData.OneLoginUserCount,
			channelUserLoginData.OneLoginEffectiveUserCount,
			channelUserLoginData.Date,
			channelUserLoginData.DateTime,
		)
	}

	if err != nil {
		totalErrorCount++
		taskErrorCount++
		failureLogger.Output(0, fmt.Sprintf("Data:%v Error:%v", channelUserLoginData, err))
	} else {
		totalSuccessCount++
		taskSuccessCount++
	}

	taskCount++
	totalCount++

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

func inArray(needle string, haystack []string) bool {
	for _, value := range haystack {
		if value == needle {
			return true
		}
	}

	return false
}

func intersect(slice1, slice2 []string) []string {
	m := make(map[string]int)
	nn := make([]string, 0)
	if len(slice1) <= len(slice2) {
		for _, v := range slice1 {
			m[v]++
		}

		for _, v := range slice2 {
			times, ok := m[v]
			if ok && times > 0 {
				nn = append(nn, v)
			}
		}
	} else {
		for _, v := range slice2 {
			m[v]++
		}

		for _, v := range slice1 {
			times, ok := m[v]
			if ok && times > 0 {
				nn = append(nn, v)
			}
		}
	}
	return nn
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

func initLog() {

	myOS := os.Getenv("OS")
	if myOS == "Windows_NT" {
		logDirPath = "./log"
	} else {
		path, _ := filepath.Abs(filepath.Dir(os.Args[0]))
		logDirPath = fmt.Sprintf("%s/log", path)
	}

	//create log dir
	_, err := os.Stat(logDirPath)
	if os.IsNotExist(err) {
		os.Mkdir(logDirPath, os.ModePerm)
	}

	//log
	failureLogFile, _ := os.OpenFile(fmt.Sprintf("%s/cul_failure-%s.log", logDirPath, date), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	failureLogger = log.New(failureLogFile, "", log.Ldate|log.Ltime)

	beginLog()
}

func beginLog() {
	failureLogger.Output(0, "\n\n========== Begin ==========")
}

func endLog() {
	failureLogger.Output(0, "\n========== End ==========\n\n")
}
