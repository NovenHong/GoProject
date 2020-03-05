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

type OrderUserData struct {
	UserId         int64   `db:"user_id"`
	Username       string  `db:"username"`
	GameId         int64   `db:"game_id"`
	ServerId       int64   `db:"server_id"`
	ChargeSum      float32 `db:"charge_sum"`
	LastChargeTime int     `db:"last_charge_time"`
}

type UserRegionData struct {
	UserId       int64   `db:"user_id"`
	Username     string  `db:"username"`
	DayDate      string  `db:"day_date"`
	DayChargeSum float32 `db:"sum"`
	Region       int     `db:"region"`
}

type OrderRegionUserData struct {
	UserId         int64
	Username       string
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

var DB *sql.DB
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

	flag.IntVar(&myPageNum, "my-page-num", 0, "每页数量")
	flag.IntVar(&currentPage, "current-page", 0, "当前页码")
	flag.Parse()
}

func main() {
	fmt.Println(fmt.Sprintf("Task begin RunDate:%s", time.Now().Format("2006-01-02 15:04:05")))

	allStartTime := time.Now().Unix()

	count := getTotalUserCount()

	pageNum := 100000
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

		userDatas := getUserDatas(startLimit, endLimit)

		var orderRegionUserDatas []OrderRegionUserData

		for _, userData := range userDatas {
			orderUserData := getOrderUserData(userData.UserId)
			if orderUserData.ChargeSum == 0 {
				continue
			}

			userRegionData := getUserRegion(userData.UserId)
			if userRegionData.Region == 0 {
				continue
			}

			orderRegionUserData := new(OrderRegionUserData)
			orderRegionUserData.UserId = userData.UserId
			orderRegionUserData.Username = userData.Username
			orderRegionUserData.GameId = orderUserData.GameId
			orderRegionUserData.ServerId = orderUserData.ServerId
			orderRegionUserData.LastLoginTime = userData.LastLoginTime
			orderRegionUserData.ChargeSum = orderUserData.ChargeSum
			orderRegionUserData.LastChargeTime = orderUserData.LastChargeTime
			orderRegionUserData.Region = userRegionData.Region
			orderRegionUserData.DayDate = userRegionData.DayDate
			orderRegionUserData.DayChargeSum = userRegionData.DayChargeSum

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
	querySql := fmt.Sprintf(`SELECT id FROM gc_gmorder_region_user WHERE user_id = %d LIMIT 1`, orderRegionUserData.UserId)

	row := DB.QueryRow(querySql)
	row.Scan(&id)

	return
}

func saveOrderRegionUserData(orderRegionUserData OrderRegionUserData) {
	var err error

	if id := isExistOrderRegionUserData(orderRegionUserData); id == 0 {
		_, err = DB.Exec(`INSERT INTO gc_gmorder_region_user(user_id,username,game_id,server_id,region,charge_sum,last_login_time,last_charge_time,day_date,day_charge_sum) 
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
			orderRegionUserData.UserId,
			orderRegionUserData.Username,
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

func getUserRegion(userId int64) (userRegionData UserRegionData) {
	querySql := fmt.Sprintf(`SELECT user_id,username,FROM_UNIXTIME(create_time,'%%Y-%%m-%%d') day_date,SUM(money/100) sum 
	FROM gc_order WHERE status=1 AND channel=1 AND user_id=%d GROUP BY day_date ORDER BY sum DESC LIMIT 1`, userId)

	row := DB.QueryRow(querySql)
	row.Scan(&userRegionData.UserId, &userRegionData.Username, &userRegionData.DayDate, &userRegionData.DayChargeSum)

	//用户付费区间 1: 6-98 2: 99-328 3: 329-647 4: 648-2000 5: 2001-5000 6: 5001+
	if userRegionData.DayChargeSum >= 6 && userRegionData.DayChargeSum <= 98 {
		userRegionData.Region = 1
	}
	if userRegionData.DayChargeSum >= 99 && userRegionData.DayChargeSum <= 328 {
		userRegionData.Region = 2
	}
	if userRegionData.DayChargeSum >= 329 && userRegionData.DayChargeSum <= 647 {
		userRegionData.Region = 3
	}
	if userRegionData.DayChargeSum >= 648 && userRegionData.DayChargeSum <= 2000 {
		userRegionData.Region = 4
	}
	if userRegionData.DayChargeSum >= 2001 && userRegionData.DayChargeSum <= 5000 {
		userRegionData.Region = 5
	}
	if userRegionData.DayChargeSum >= 5001 {
		userRegionData.Region = 6
	}

	return
}

func getOrderUserData(userId int64) (orderUserData OrderUserData) {
	querySql := fmt.Sprintf(`SELECT user_id,username,sum(money/100) charge_sum,max(create_time) last_charge_time,
	substring_index(group_concat(distinct game_id order by create_time desc),',',1) game_id,
	substring_index(group_concat(distinct game_server_id order by create_time desc),',',1) server_id
	FROM gc_order WHERE (status = 1) AND (channel = 1) AND (create_time > 1556640000) AND (user_id = %d)`, userId)

	row := DB.QueryRow(querySql)
	row.Scan(
		&orderUserData.UserId,
		&orderUserData.Username,
		&orderUserData.ChargeSum,
		&orderUserData.LastChargeTime,
		&orderUserData.GameId,
		&orderUserData.ServerId,
	)

	return
}

func getUserDatas(startLimit int, endLimit int) (userDatas []UserData) {
	querySql := fmt.Sprintf(`SELECT user_id,username,last_login_time FROM gc_user LIMIT %d,%d`, startLimit, endLimit)

	rows, err := DB.Query(querySql)

	if err != nil {
		fmt.Println(err)
		return
	}

	userData := new(UserData)

	for rows.Next() {
		rows.Scan(&userData.UserId, &userData.Username, &userData.LastLoginTime)
		userDatas = append(userDatas, *userData)
	}

	defer func() {
		rows.Close()
	}()

	return
}

func getTotalUserCount() (count int) {
	querySql := `SELECT count(*) count FROM gc_user LIMIT 1`

	row := DB.QueryRow(querySql)
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

func resolveSecond(second int64) (time string) {

	minute := second / 60

	hour := minute / 60

	minute = minute % 60

	second = second - hour*3600 - minute*60

	time = fmt.Sprintf("%d:%d:%d", hour, minute, second)

	//fmt.Println(time)

	return
}
