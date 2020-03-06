package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"math"
	"os"
	"time"
)

type UserData struct {
	UserId        int64  `db:"user_id"`
	Username      string `db:"username"`
	LastLoginTime int    `db:"last_login_time"`
}

type UserRoleData struct {
	Username string `db:"username"`
	RoleId   string `db:"role_id"`
}

type UserRoleOrderData struct {
	UserId         int64   `db:"user_id"`
	Username       string  `db:"username"`
	RoleId         string  `db:"role_id"`
	GameId         int64   `db:"game_id"`
	ServerId       int64   `db:"server_id"`
	ChargeSum      float32 `db:"charge_sum"`
	LastChargeTime int     `db:"last_charge_time"`
}

type UserRoleRegionData struct {
	UserId       int64   `db:"user_id"`
	Username     string  `db:"username"`
	DayDate      string  `db:"day_date"`
	DayChargeSum float32 `db:"sum"`
	Region       int     `db:"region"`
}

type OrderRegionUserData struct {
	UserId         int64
	Username       string
	RoleId         string
	GameId         int64
	ServerId       int64
	Region         int
	ChargeSum      float32
	LastLoginTime  int
	LastChargeTime int
	DayDate        string
	DayChargeSum   float32
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
	USERNAME2 string = "cj655"
	PASSWORD2 string = "game123456" //game123456
	NETWORK2  string = "tcp"
	SERVER2   string = "120.132.31.31"
	PORT2     int    = 3306
	DATABASE2 string = "cj655"
)

var DB *sql.DB
var DB2 *sql.DB
var myPageNum int
var currentPage int
var totalCount int64
var totalSuccessCount int64
var totalErrorCount int64

func init() {
	myOS := os.Getenv("OS")
	if myOS == "Windows_NT" {
		PASSWORD = ""
	} else {
		PASSWORD = "game123456"
	}

	DB = openDB()
	DB2 = openDB2()

	flag.IntVar(&myPageNum, "my-page-num", 0, "每页数量")
	flag.IntVar(&currentPage, "current-page", 0, "当前页码")
	flag.Parse()
}

func main() {
	fmt.Println(fmt.Sprintf("Task begin RunDate:%s", time.Now().Format("2006-01-02 15:04:05")))

	allStartTime := time.Now().Unix()

	count := getTotalUserRoleCount()

	pageNum := 10000
	if myPageNum > 0 {
		pageNum = myPageNum
	}

	totalPage := math.Ceil(float64(count) / float64(pageNum))

	for i := 1; i <= int(totalPage); i++ {

		if currentPage > 0 && i != currentPage {
			continue
		}

		startLimit := (i - 1) * pageNum
		endLimit := pageNum

		userRoleDatas := getUserRoleDatas(startLimit, endLimit)

		var orderRegionUserDatas []OrderRegionUserData

		for _, userRoleData := range userRoleDatas {

			userData := getUserData(userRoleData.Username)
			if userData.UserId == 0 {
				continue
			}

			userRoleOrderData := getUserRoleOrderData(userRoleData.RoleId)
			if userRoleOrderData.ChargeSum == 0 {
				continue
			}

			userRoleRegionData := getUserRoleRegionData(userRoleData.RoleId)
			if userRoleRegionData.Region == 0 {
				continue
			}

			orderRegionUserData := new(OrderRegionUserData)
			orderRegionUserData.UserId = userData.UserId
			orderRegionUserData.Username = userData.Username
			orderRegionUserData.RoleId = userRoleData.RoleId
			orderRegionUserData.GameId = userRoleOrderData.GameId
			orderRegionUserData.ServerId = userRoleOrderData.ServerId
			orderRegionUserData.LastLoginTime = userData.LastLoginTime
			orderRegionUserData.ChargeSum = userRoleOrderData.ChargeSum
			orderRegionUserData.LastChargeTime = userRoleOrderData.LastChargeTime
			orderRegionUserData.Region = userRoleRegionData.Region
			orderRegionUserData.DayDate = userRoleRegionData.DayDate
			orderRegionUserData.DayChargeSum = userRoleRegionData.DayChargeSum

			orderRegionUserDatas = append(orderRegionUserDatas, *orderRegionUserData)
		}

		for _, orderRegionUserData := range orderRegionUserDatas {
			saveOrderRegionUserData(orderRegionUserData)
		}
	}

	allEndTime := time.Now().Unix()

	fmt.Println(fmt.Sprintf("All task is compeleted,SuccessRow:%d ErrorRow:%d TotalRow:%d Time:%s",
		totalSuccessCount, totalErrorCount, totalCount, resolveSecond(allEndTime-allStartTime)))
}

func isExistOrderRegionUserData(orderRegionUserData OrderRegionUserData) (id int64) {
	querySql := fmt.Sprintf(`SELECT id FROM gc_gmorder_region_user WHERE role_id = '%s' LIMIT 1`, orderRegionUserData.RoleId)

	row := DB.QueryRow(querySql)
	row.Scan(&id)

	return
}

func saveOrderRegionUserData(orderRegionUserData OrderRegionUserData) {
	var err error

	if id := isExistOrderRegionUserData(orderRegionUserData); id == 0 {
		_, err = DB.Exec(`INSERT INTO gc_gmorder_region_user(user_id,username,role_id,game_id,server_id,region,charge_sum,last_login_time,last_charge_time,day_date,day_charge_sum) 
		VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
			orderRegionUserData.UserId,
			orderRegionUserData.Username,
			orderRegionUserData.RoleId,
			orderRegionUserData.GameId,
			orderRegionUserData.ServerId,
			orderRegionUserData.Region,
			orderRegionUserData.ChargeSum,
			orderRegionUserData.LastLoginTime,
			orderRegionUserData.LastChargeTime,
			orderRegionUserData.DayDate,
			orderRegionUserData.DayChargeSum,
		)
	} else {
		_, err = DB.Exec(`UPDATE gc_gmorder_region_user SET server_id = ?,region = ?,charge_sum = ?,last_login_time = ?,
		last_charge_time = ?,day_date = ?,day_charge_sum = ? WHERE id=?`,
			orderRegionUserData.ServerId,
			orderRegionUserData.Region,
			orderRegionUserData.ChargeSum,
			orderRegionUserData.LastLoginTime,
			orderRegionUserData.LastChargeTime,
			orderRegionUserData.DayDate,
			orderRegionUserData.DayChargeSum,
			id,
		)
	}

	if err != nil {
		totalErrorCount++
		fmt.Println(err)
	} else {
		totalSuccessCount++
	}

	totalCount++
}

func getUserRoleRegionData(roleId string) (userRoleRegionData UserRoleRegionData) {
	querySql := fmt.Sprintf(`SELECT user_id,username,FROM_UNIXTIME(create_time,'%%Y-%%m-%%d') day_date,SUM(money/100) sum 
	FROM gc_order WHERE status=1 AND channel=1 AND role_id = '%s' GROUP BY day_date ORDER BY sum DESC LIMIT 1`, roleId)

	row := DB.QueryRow(querySql)
	row.Scan(&userRoleRegionData.UserId, &userRoleRegionData.Username, &userRoleRegionData.DayDate, &userRoleRegionData.DayChargeSum)

	//用户付费区间 1: 6-98 2: 99-328 3: 329-647 4: 648-2000 5: 2001-5000 6: 5001+
	if userRoleRegionData.DayChargeSum >= 6 && userRoleRegionData.DayChargeSum <= 98 {
		userRoleRegionData.Region = 1
	}
	if userRoleRegionData.DayChargeSum >= 99 && userRoleRegionData.DayChargeSum <= 328 {
		userRoleRegionData.Region = 2
	}
	if userRoleRegionData.DayChargeSum >= 329 && userRoleRegionData.DayChargeSum <= 647 {
		userRoleRegionData.Region = 3
	}
	if userRoleRegionData.DayChargeSum >= 648 && userRoleRegionData.DayChargeSum <= 2000 {
		userRoleRegionData.Region = 4
	}
	if userRoleRegionData.DayChargeSum >= 2001 && userRoleRegionData.DayChargeSum <= 5000 {
		userRoleRegionData.Region = 5
	}
	if userRoleRegionData.DayChargeSum >= 5001 {
		userRoleRegionData.Region = 6
	}

	return
}

func getUserRoleOrderData(roleId string) (userRoleOrderData UserRoleOrderData) {
	querySql := fmt.Sprintf(`SELECT user_id,username,sum(money/100) charge_sum,max(create_time) last_charge_time,game_id,role_id, 
	substring_index(group_concat(distinct game_server_id order by create_time desc),',',1) server_id
	FROM gc_order WHERE (status = 1) AND (channel = 1) AND (create_time > 1556640000) AND (role_id = '%s')`, roleId)

	row := DB.QueryRow(querySql)
	row.Scan(
		&userRoleOrderData.UserId,
		&userRoleOrderData.Username,
		&userRoleOrderData.ChargeSum,
		&userRoleOrderData.LastChargeTime,
		&userRoleOrderData.GameId,
		&userRoleOrderData.RoleId,
		&userRoleOrderData.ServerId,
	)

	return
}

func getUserData(username string) (userData UserData) {
	querySql := fmt.Sprintf(`SELECT user_id,username,last_login_time FROM gc_user WHERE username = '%s'`, username)

	row := DB.QueryRow(querySql)

	row.Scan(&userData.UserId, &userData.Username, &userData.LastLoginTime)

	return
}

func getUserRoleDatas(startLimit int, endLimit int) (userRoleDatas []UserRoleData) {
	querySql := fmt.Sprintf(`SELECT username,role_id FROM gc_user_role LIMIT %d,%d`, startLimit, endLimit)

	rows, err := DB2.Query(querySql)

	if err != nil {
		fmt.Println(err)
		return
	}

	userRoleData := new(UserRoleData)

	for rows.Next() {
		rows.Scan(&userRoleData.Username, &userRoleData.RoleId)
		userRoleDatas = append(userRoleDatas, *userRoleData)
	}

	defer func() {
		rows.Close()
	}()

	return
}

func getTotalUserRoleCount() (count int) {
	querySql := `SELECT count(*) count FROM gc_user_role LIMIT 1`

	row := DB2.QueryRow(querySql)
	row.Scan(&count)

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

func resolveSecond(second int64) (time string) {

	minute := second / 60

	hour := minute / 60

	minute = minute % 60

	second = second - hour*3600 - minute*60

	time = fmt.Sprintf("%d:%d:%d", hour, minute, second)

	//fmt.Println(time)

	return
}
