package main

import (
	"fmt"
	"time"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"flag"
	"net/http"
	"io/ioutil"
	"github.com/techoner/gophp/serialize"
	"os"
	"encoding/json"
	"path/filepath"
	"log"
	"strconv"
	"strings"
	"math"
	"regexp"
	//"os/exec"
)

const (
    USERNAME = "root"
    //PASSWORD = ""//game123456
    NETWORK  = "tcp"
    SERVER   = "localhost"
    PORT     = 3306
    DATABASE = "cj655"
)

type OrderData struct {
	ChannelId string `db:"channel_id"`
	ChargeSum float64 `db:"charge_sum"`
	ChargeNum int `db:"charge_num"`
}

type NewOrderData struct {
	ChannelId string `db:"channel_id"`
	NewChargeSum float64 `db:"new_charge_sum"`
	NewChargeNum int `db:"new_charge_num"`
}

type UserData struct {
	ChannelId string `db:"channel_id"`
	RegNum int `db:"reg_num"`
}

type UserRoleData struct {
	ChannelId string `json:"user_channel_id"`
	EffectiveNum string `json:"effective_num"`
}

type ChannelData struct {
	ChannelId string `json:"channel_id"`
}

type ChannelMonthCountData struct {
	Id int64
	ChannelId int64
	ChargeSum float64
	ChargeNum int
	NewChargeSum float64
	NewChargeNum int
	RegNum int
	EffectiveNum int
	EffectiveNum130_149 int
	LoginCount int
	NewLoginCount int
	DateTime int64
	Date string
}

var PASSWORD string = ""

var logDirPath string
var failureLogger *log.Logger

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
var dayOffset int
var channelDatas []ChannelData

var totalCount int64 = 0
var totalSuccessCount int64 = 0
var totalErrorCount int64 = 0

var taskCount int64 = 0
var taskSuccessCount int64 = 0
var taskErrorCount int64 = 0

var allStartTime int64
var allEndTime int64

var DB *sql.DB
var err error
var maxConnections int
var waitDBNotBusyCount int
var waitDBNotBusyTimeout int

func init() {
	myOS := os.Getenv("OS")
	if myOS == "Windows_NT" {
		PASSWORD = ""
	}else{
		PASSWORD = "game123456"
	}

	DB = openDB()

	loc, _ = time.LoadLocation("Local")

	months = []string{}

	months = append(months,time.Now().Format("2006-01"))

	//1号统计上月数据
	day := time.Now().Day()
	if day == 1 {
		curMonth := int(time.Now().Month())
		lastMonth := (curMonth-1)
		if lastMonth <= 0 {
			lastMonth = 12
		}
		months = append(months,fmt.Sprintf("%d-%d",time.Now().Year(),lastMonth))
	}

	initFlag()

}

func initTime() {

	if dayOffset == 7 {
		startTimeDate = regexp.MustCompile("([0-9]{4})-([0-9]{2})-([0-9]{2})").ReplaceAllString(startTimeDate, "$1-$2-07")
	}

	if dayOffset == 10 {
		startTimeDate = regexp.MustCompile("([0-9]{4})-([0-9]{2})-([0-9]{2})").ReplaceAllString(startTimeDate, "$1-$2-10")
	}

	//startTime :=  time.Now().AddDate(0, -2, 0).Format("2006-01-02 15:04:05")
	theTime, _ := time.ParseInLocation("2006-01-02", startTimeDate, loc)
	startTime = theTime.Unix()

	//endTimeDate := time.Now().Format("2006-01-02")
	theTime, _ = time.ParseInLocation("2006-01-02", endTimeDate, loc)
	endTime = theTime.Unix()+86399

	date = theTime.Format("2006-01")

	theTime2, _ := time.ParseInLocation("2006-01", date, loc)

	dateTime = theTime2.Unix()
}

func main()  {

	allStartTime = time.Now().Unix()

	channelDatas = getChannelIds()

	for _,month := range months {

		monthDays := getFirstLastMonthDay(month)
		startTimeDate = monthDays["firstDay"]
		endTimeDate = monthDays["lastDay"]

		initTime()

		initLog()

		//fmt.Println(time.Unix(startTime,0).Format("2006-01-02 15:04:05"))
		//fmt.Println(time.Unix(endTime,0).Format("2006-01-02 15:04:05"))
	
		fmt.Println(fmt.Sprintf("Task:%s begin StartDate:%s EndDate:%s RunDate:%s",date,startTimeDate,endTimeDate,time.Now().Format("2006-01-02 15:04:05")))

		startTask()

		time.Sleep(time.Second*5)

		endLog()
	}

	allEndTime = time.Now().Unix()

	fmt.Println(fmt.Sprintf("All task is compeleted,SuccessRow:%d ErrorRow:%d TotalRow:%d Time:%s",
	totalSuccessCount,totalErrorCount,totalCount,resolveSecond(allEndTime-allStartTime)))

	// _,err := exec.Command("bash","-c","kill -USR1 `ps -ef | grep '/usr/bin/python.*cm' | grep -v 'grep' | awk '{print $2}'`").CombinedOutput()
	// if err != nil {
	// 	fmt.Println(fmt.Sprintf("Send signal error,Error:%s",err))
	// }
}

func startTask() {

	//whether database is busy
	for {
		if isDBBusy() {
			DB.Close()
			waitDBNotBusyCount++
			waitTime := time.Second*time.Duration(math.Pow(5,float64(waitDBNotBusyCount)))
			fmt.Println(fmt.Sprintf("Database is busy,WaitCount:%d WaitTime:%v",waitDBNotBusyCount,waitTime))
			failureLogger.Output(0,fmt.Sprintf("Database is busy,WaitCount:%d WaitTime:%v",waitDBNotBusyCount,waitTime))
			time.Sleep(waitTime)
			openDB()
		}else {
			waitDBNotBusyCount = 0
			break
		}
		
	}

	taskStartTime := time.Now().Unix()

	orderDatas := getChargeSumAndChargeNum()

	time.Sleep(time.Second * 1)

	newOrderDatas := getNewChargeSumAndNewChargeNum()

	time.Sleep(time.Second * 1)

	userDatas := getRegNum()

	time.Sleep(time.Second * 1)

	userRoleDatas := getEffectiveNum()

	time.Sleep(time.Second * 1)

	userRoleDatas2 := get130_149_EffectiveNum()

	time.Sleep(time.Second * 1)

	taskCount = 0
	taskSuccessCount = 0
	taskErrorCount = 0

	for _,channelData := range channelDatas {

		channelMonthCountData := new(ChannelMonthCountData)

		channelMonthCountData.ChannelId = int64(myAtoi(channelData.ChannelId))

		channelMonthCountData.Date = date

		channelMonthCountData.DateTime = dateTime

		for _,value := range orderDatas {
			if value.ChannelId == channelData.ChannelId {
				channelMonthCountData.ChargeSum = value.ChargeSum
				channelMonthCountData.ChargeNum = value.ChargeNum
				break
			}
		}

		for _,value := range newOrderDatas {
			if value.ChannelId == channelData.ChannelId {
				channelMonthCountData.NewChargeSum = value.NewChargeSum
				channelMonthCountData.NewChargeNum = value.NewChargeNum
				break
			}
		}

		for _,value := range userDatas {
			if value.ChannelId == channelData.ChannelId {
				channelMonthCountData.RegNum = value.RegNum
				break
			}
		}

		for _,value := range userRoleDatas {
			if value.ChannelId == channelData.ChannelId {
				channelMonthCountData.EffectiveNum = myAtoi(value.EffectiveNum)
				break
			}
		}

		for _,value := range userRoleDatas2 {
			if value.ChannelId == channelData.ChannelId {
				channelMonthCountData.EffectiveNum130_149 = myAtoi(value.EffectiveNum)
				break
			}
		}

		channelMonthCountData.LoginCount = getLoginCount(channelMonthCountData.ChannelId)

		channelMonthCountData.NewLoginCount = getNewLoginCount(channelMonthCountData.ChannelId)

		if id := isExistChannelMonthCountData(*channelMonthCountData); id > 0 {
			channelMonthCountData.Id = id
		}

		//fmt.Println(channelMonthCountData)
		//continue

		saveChannelMonthCountData(*channelMonthCountData)

	}

	taskEndTime := time.Now().Unix()

	fmt.Println(fmt.Sprintf("Task:%s is compeleted,SuccessRow:%d ErrorRow:%d TotalRow:%d Time:%s",
	date,taskSuccessCount,taskErrorCount,taskCount,resolveSecond(taskEndTime-taskStartTime)))

}

func openDB() (DB *sql.DB) {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s",USERNAME,PASSWORD,NETWORK,SERVER,PORT,DATABASE)
	DB,err = sql.Open("mysql",dsn)
	
	if err != nil{
        panic(fmt.Sprintf("Open mysql failed,Error:%v\n",err))
	}

	DB.SetConnMaxLifetime(100*time.Second)  //最大连接周期，超过时间的连接就close
    DB.SetMaxOpenConns(100)//设置最大连接数
	DB.SetMaxIdleConns(16) //设置闲置连接数

	if maxConnections == 0 {
		var variableName string
		row := DB.QueryRow(`show variables like "max_connections"`)
		row.Scan(&variableName,&maxConnections);
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
	row.Scan(&processlistCount);
	return
}

func initFlag() {
	var currentMonth string
	flag.StringVar(&currentMonth,"month","","当前的月份")
	flag.IntVar(&limit,"limit",0,"全部条数")
	flag.IntVar(&dayOffset,"offset",0,"月份天数偏移量")
	flag.Parse()

	if currentMonth != "" {
		months = strings.Split(currentMonth,",")
	}
}

func getFirstLastMonthDay(monthDate string) (monthDays map[string]string) {
	theTime, _ := time.ParseInLocation("2006-01", monthDate, loc)
	year,month,_ := theTime.Date()

	firstMonthUTC :=time.Date(year,month,1,0,0,0,0,loc)
	firstMonthDay := firstMonthUTC.Format("2006-01-02")
	lastMonthDay := firstMonthUTC.AddDate(0, 1, -1).Format("2006-01-02")

	monthDays = map[string]string{}

	monthDays["firstDay"] = firstMonthDay
	monthDays["lastDay"] = lastMonthDay

	return
}


func getChargeSumAndChargeNum() (orderDatas []OrderData) {

	quarySql = fmt.Sprintf(`SELECT round(sum(o.money/100),2) charge_sum,count(distinct o.user_id) charge_num,u.channel_id 
	FROM gc_user as u LEFT JOIN gc_order as o on u.user_id = o.user_id 
	WHERE ( o.status = 1 ) AND ( o.channel = 1 ) AND ( (o.create_time BETWEEN %d AND %d ) ) AND ( u.channel_id is not null ) GROUP BY u.channel_id`,startTime,endTime)

	rows, err := DB.Query(quarySql)

	if err != nil {
		//fmt.Println(fmt.Sprintf("Sql:%s Error:%v",quarySql,err))
		failureLogger.Output(0,fmt.Sprintf("Sql:%s Error:%v",quarySql,err))
		return
	}

	orderData := new(OrderData)

	orderDatas = []OrderData{}

	for rows.Next() {
		rows.Scan(&orderData.ChargeSum,&orderData.ChargeNum,&orderData.ChannelId)
		orderDatas = append(orderDatas,*orderData)
	}

	defer func() {
		rows.Close()
	}()

	return
}

func getNewChargeSumAndNewChargeNum() (newOrderDatas []NewOrderData) {

	quarySql = fmt.Sprintf(`SELECT round(sum(o.money/100),2) new_charge_sum,count(distinct o.user_id) new_charge_num,u.channel_id
	FROM gc_user as u LEFT JOIN gc_order as o on u.user_id = o.user_id 
	WHERE ( o.status = 1 ) AND ( o.channel = 1 ) AND ( (o.create_time BETWEEN %d AND %d ) ) 
AND ( (u.reg_time BETWEEN %d AND %d ) ) AND ( u.channel_id is not null ) GROUP BY u.channel_id`,startTime,endTime,startTime,endTime)

	rows, err := DB.Query(quarySql)

	if err != nil {
		//fmt.Println(fmt.Sprintf("Sql:%s Error:%v",quarySql,err))
		failureLogger.Output(0,fmt.Sprintf("Sql:%s Error:%v",quarySql,err))
		return
	}

	newOrderData := new(NewOrderData)

	newOrderDatas = []NewOrderData{}

	for rows.Next() {
		rows.Scan(&newOrderData.NewChargeSum,&newOrderData.NewChargeNum,&newOrderData.ChannelId)
		newOrderDatas = append(newOrderDatas,*newOrderData)
	}

	defer func() {
		rows.Close()
	}()

	return
}

func getRegNum() (userDatas []UserData){
	quarySql = fmt.Sprintf(`SELECT count(user_id) reg_num,channel_id FROM gc_user 
	WHERE ( (reg_time BETWEEN %d AND %d ) ) AND ( channel_id is not null ) GROUP BY channel_id`,startTime,endTime)

	rows, err := DB.Query(quarySql)

	if err != nil {
		//fmt.Println(fmt.Sprintf("Sql:%s Error:%v",quarySql,err))
		failureLogger.Output(0,fmt.Sprintf("Sql:%s Error:%v",quarySql,err))
		return
	}

	userData := new(UserData)

	userDatas = []UserData{}

	for rows.Next() {
		rows.Scan(&userData.RegNum,&userData.ChannelId)
		userDatas = append(userDatas,*userData)
	}

	defer func() {
		rows.Close()
	}()

	return
}

func getEffectiveNum() (userRoleDatas []UserRoleData) {

	where := fmt.Sprintf("is_effective = 1 AND dabiao_time BETWEEN %d AND %d",startTime,endTime)

	where2,_ := serialize.Marshal(where)

	where3 := string(where2)

	field := "user_channel_id,count(distinct username) effective_num"

	group := "user_channel_id"

	url := fmt.Sprintf("http://dj.cj655.com/api.php?m=player&a=admin_role_array7&where=%s&field=%s&group=%s",where3,field,group)

	resp, err := http.Get(url)

	if err != nil {
		//fmt.Println(err)
		failureLogger.Output(0,fmt.Sprintf("Error:%v",err))
        return
	}
	
	body, _ := ioutil.ReadAll(resp.Body)

	_ = json.Unmarshal(body,&userRoleDatas)

	return
}

func get130_149_EffectiveNum() (userRoleDatas []UserRoleData) {
	where := fmt.Sprintf("(is_effective = 1) AND (dabiao_time BETWEEN %d AND %d) AND (level BETWEEN 130 AND 149)",startTime,endTime)

	where2,_ := serialize.Marshal(where)

	where3 := string(where2)

	field := "user_channel_id,count(distinct username) effective_num"

	group := "user_channel_id"

	url := fmt.Sprintf("http://dj.cj655.com/api.php?m=player&a=admin_role_array7&where=%s&field=%s&group=%s",where3,field,group)

	resp, err := http.Get(url)

	if err != nil {
		//fmt.Println(err)
		failureLogger.Output(0,fmt.Sprintf("Error:%v",err))
        return
	}

	body, _ := ioutil.ReadAll(resp.Body)

	_ = json.Unmarshal(body,&userRoleDatas)

	return
}

func getLoginCount(channelId int64) (loginCount int) {

	quarySql = fmt.Sprintf(`SELECT COUNT(distinct upd.user_id) login_count 
	FROM gc_user_play_data as upd LEFT JOIN gc_user as u on upd.user_id = u.user_id 
	WHERE (upd.login_time BETWEEN %d AND %d ) AND ( u.channel_id = %d )`,startTime,endTime,channelId)

	row := DB.QueryRow(quarySql)

	err = row.Scan(&loginCount)

	if err != nil {
		failureLogger.Output(0,fmt.Sprintf("Sql:%s Error:%v",quarySql,err))
		return
	}

	return
}

func getNewLoginCount(channelId int64) (newLoginCount int) {

	quarySql = fmt.Sprintf(`SELECT COUNT(distinct upd.user_id) login_count 
	FROM gc_user_play_data as upd LEFT JOIN gc_user as u on upd.user_id = u.user_id 
	WHERE (upd.login_time BETWEEN %d AND %d) AND (u.reg_time BETWEEN %d AND %d) AND ( u.channel_id = %d )`,startTime,endTime,startTime,endTime,channelId)

	row := DB.QueryRow(quarySql)

	err = row.Scan(&newLoginCount)

	if err != nil {
		failureLogger.Output(0,fmt.Sprintf("Sql:%s Error:%v",quarySql,err))
		return
	}

	return
}

func getChannelIds() (channelDatas []ChannelData) {
	where := "(status > 0) AND (channel_is_delete = 0) AND (channel_id > 0)"

	where2,_ := serialize.Marshal(where)

	where3 := string(where2)

	field := "channel_id"

	url := fmt.Sprintf("https://www.cj655.com/api.php?m=channelpublic&a=channel_data&where=%s&field=%s&api_key=TbjoLfLhnikp92hyd8dx0ozCcEipII2Z",where3,field)
	
	if limit > 0 {
		url = fmt.Sprintf("https://www.cj655.com/api.php?m=channelpublic&a=channel_data&where=%s&field=%s&limit=%d&api_key=TbjoLfLhnikp92hyd8dx0ozCcEipII2Z",where3,field,limit)
	}

	//fmt.Println(url)

	resp, err := http.Get(url)

	if err != nil {
		failureLogger.Output(0,fmt.Sprintf("Error:%v",err))
        return
	}

	body, _ := ioutil.ReadAll(resp.Body)

	_ = json.Unmarshal(body,&channelDatas)

	return
}


func isExistChannelMonthCountData(channelMonthCountData ChannelMonthCountData) (id int64) {
	quarySql2 := fmt.Sprintf(`Select id 
	FROM %s WHERE channel_id = %d AND date = '%s' LIMIT 1`,
	getTableName(),
	channelMonthCountData.ChannelId,
	channelMonthCountData.Date,
	)

	row := DB.QueryRow(quarySql2)
	row.Scan(&id);

	return 
}

func getTableName() (tableName string) {

	tableName = "gc_channel_month_count"

	if dayOffset == 7 || dayOffset == 10 {
		tableName = fmt.Sprintf("gc_channel_month_count_egt_%d",dayOffset)
	}

	return
}


func saveChannelMonthCountData(channelMonthCountData ChannelMonthCountData) {

	var err error

	if channelMonthCountData.Id > 0 {
		quarySql3 := fmt.Sprintf(`UPDATE %s SET 
		charge_sum=?,charge_num=?,new_charge_sum=?,new_charge_num=?,reg_num=?,effective_num=?,effective_num130_149=?,login_count=?,new_login_count=?
		WHERE id=?`,getTableName())
		_,err = DB.Exec(
			quarySql3,
			channelMonthCountData.ChargeSum,
			channelMonthCountData.ChargeNum,
			channelMonthCountData.NewChargeSum,
			channelMonthCountData.NewChargeNum,
			channelMonthCountData.RegNum,
			channelMonthCountData.EffectiveNum,
			channelMonthCountData.EffectiveNum130_149,
			channelMonthCountData.LoginCount,
			channelMonthCountData.NewLoginCount,
			channelMonthCountData.Id,
		)
	}else{
		quarySql3 := fmt.Sprintf(`insert INTO %s
		(channel_id,charge_sum,charge_num,new_charge_sum,new_charge_num,reg_num,effective_num,effective_num130_149,login_count,new_login_count,date_time,date)
		values(?,?,?,?,?,?,?,?,?,?,?,?)`,getTableName())
		_,err = DB.Exec(
			quarySql3,
			channelMonthCountData.ChannelId,
			channelMonthCountData.ChargeSum,
			channelMonthCountData.ChargeNum,
			channelMonthCountData.NewChargeSum,
			channelMonthCountData.NewChargeNum,
			channelMonthCountData.RegNum,
			channelMonthCountData.EffectiveNum,
			channelMonthCountData.EffectiveNum130_149,
			channelMonthCountData.LoginCount,
			channelMonthCountData.NewLoginCount,
			channelMonthCountData.DateTime,
			channelMonthCountData.Date,
		)
	}


	if err != nil{
		totalErrorCount++
		taskErrorCount++
		//fmt.Println(fmt.Sprintf("Data:%v Error:%v",channelMonthCountData,err))
		failureLogger.Output(0,fmt.Sprintf("Data:%v Error:%v",channelMonthCountData,err))
	}else{
		totalSuccessCount++
		taskSuccessCount++
	}

	totalCount++
	taskCount++

}

func myAtoi(s string) (i int) {
	i,_ = strconv.Atoi(s)
	return
}

func resolveSecond(second int64) (time string) {

	minute := second/60

	hour := minute/60

	minute = minute%60

	second = second-hour*3600-minute*60

	time = fmt.Sprintf("%d:%d:%d",hour,minute,second)

	//fmt.Println(time)

	return
}

func initLog() {

	myOS := os.Getenv("OS")
	if myOS == "Windows_NT" {
		logDirPath = "./log"
	}else{
		path, _ := filepath.Abs(filepath.Dir(os.Args[0]))
		logDirPath = fmt.Sprintf("%s/log",path)
	}

	//create log dir
	_, err := os.Stat(logDirPath)
	if os.IsNotExist(err) {
		os.Mkdir(logDirPath, os.ModePerm)
	}

	//log
	failureLogFile, _ := os.OpenFile(fmt.Sprintf("%s/cmc_failure-%s.log",logDirPath,date), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	failureLogger = log.New(failureLogFile,"",log.Ldate | log.Ltime)

	beginLog()
}

func beginLog() {
	failureLogger.Output(0,"\n\n========== Begin ==========")
}

func endLog() {
	failureLogger.Output(0,"\n========== End ==========\n\n")
}