package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"time"
)

type ServerData struct {
	GameId    int64 `db:"game_id"`
	ServerId  int64 `db:"game_server_id"`
	OpenTime  int64 `db:"open_time"`
	UserCount int
	Type      int
}

type UserRoleData struct {
	Username       string `db:"username"`
	RoleId         string `db:"role_id"`
	LastChargeTime int    `db:"last_charge_time"`
	LastLoginTime  int    `db:"last_login_time"`
}

type UserLifeCycleData struct {
	GameId    int64
	ServerId  int64
	Region    int
	Type      int
	UserCount int
	Date      string
	DateTime  int64
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
var loc *time.Location
var serverId int64
var totalCount int64
var totalSuccessCount int64
var totalErrorCount int64

//停充区间
//1:8~10 2:11~13 3:14~16 4:17~19 5:20~22 6:23~25 7:26~28 8:29~31 9:32~34 10:35~37
//11:38~40 12:41~43 13:44~46 14:47~49 15:50~52 16:53~55 17:56~58 18:59~61 19:62~64 20:65+
var noChargeRegionValues = map[int][]int{
	1: []int{8, 10}, 2: []int{11, 13}, 3: []int{14, 16}, 4: []int{17, 19}, 5: []int{20, 22},
	6: []int{23, 25}, 7: []int{26, 28}, 8: []int{29, 31}, 9: []int{32, 34}, 10: []int{35, 37},
	11: []int{38, 40}, 12: []int{41, 43}, 13: []int{44, 46}, 14: []int{47, 49}, 15: []int{50, 52},
	16: []int{53, 55}, 17: []int{56, 58}, 18: []int{59, 61}, 19: []int{62, 64}, 20: []int{65, 90},
}

//流失区间
//1:4~6 2:7~9 3:10~12 4:13~15 5:16~18 6:19~21 7:22~24 8:25~27 9:28~30 10: 31~33
//11:34~36 12: 37~39 13:40~42 14:43~45 15: 46~48 16:49~51 17: 52~54 18:55~57 19:58~60 20:61+
var lostRegionValues = map[int][]int{
	1: []int{4, 6}, 2: []int{7, 9}, 3: []int{10, 12}, 4: []int{13, 15}, 5: []int{16, 18},
	6: []int{19, 21}, 7: []int{22, 24}, 8: []int{25, 27}, 9: []int{28, 30}, 10: []int{31, 33},
	11: []int{34, 36}, 12: []int{37, 39}, 13: []int{40, 42}, 14: []int{43, 45}, 15: []int{46, 48},
	16: []int{49, 51}, 17: []int{52, 54}, 18: []int{55, 57}, 19: []int{58, 60}, 20: []int{61, 90},
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

	flag.Int64Var(&serverId, "server-id", 0, "单个区服计算")
	flag.Parse()
}

func main() {

	fmt.Println(fmt.Sprintf("Task begin RunDate:%s", time.Now().Format("2006-01-02 15:04:05")))

	allStartTime := time.Now().Unix()

	year, month, _ := time.Now().AddDate(0, -2, 0).Date()

	startTime := time.Date(year, month, 1, 0, 0, 0, 0, loc).Unix()

	date := time.Date(time.Now().Year(), time.Now().Month(), 1, 0, 0, 0, 0, loc)
	endTime := date.AddDate(0, 1, -1).Unix() + 86399

	serverDatas := getServerDatas(startTime, endTime)

	for i := 1; i <= 20; i++ {
		userLifeCycleDatas := getUserLifeCycleData(i, serverDatas, date)

		for _, userLifeCycleData := range userLifeCycleDatas {
			saveUserLifeCycleData(userLifeCycleData)
		}

	}

	allEndTime := time.Now().Unix()

	fmt.Println(fmt.Sprintf("All task is compeleted,SuccessRow:%d ErrorRow:%d TotalRow:%d Time:%s",
		totalSuccessCount, totalErrorCount, totalCount, resolveSecond(allEndTime-allStartTime)))

}

func isExistUserLifeCycleData(userLifeCycleData UserLifeCycleData) (id int64) {
	querySql := fmt.Sprintf(`SELECT id FROM gc_gmuser_lifecycle WHERE game_id = %d AND server_id = %d AND region = %d AND type = %d AND date = '%s' LIMIT 1`,
		userLifeCycleData.GameId, userLifeCycleData.ServerId, userLifeCycleData.Region, userLifeCycleData.Type, userLifeCycleData.Date)

	row := DB.QueryRow(querySql)
	row.Scan(&id)

	return
}

func saveUserLifeCycleData(userLifeCycleData UserLifeCycleData) {
	var err error

	if id := isExistUserLifeCycleData(userLifeCycleData); id == 0 {
		_, err = DB.Exec(`INSERT INTO gc_gmuser_lifecycle(game_id,server_id,region,type,user_count,date,date_time) VALUES(?,?,?,?,?,?,?)`,
			userLifeCycleData.GameId,
			userLifeCycleData.ServerId,
			userLifeCycleData.Region,
			userLifeCycleData.Type,
			userLifeCycleData.UserCount,
			userLifeCycleData.Date,
			userLifeCycleData.DateTime)
	} else {
		_, err = DB.Exec(`UPDATE gc_gmuser_lifecycle SET user_count = ? WHERE id = ?`, userLifeCycleData.UserCount, id)
	}

	if err != nil {
		totalErrorCount++
		fmt.Println(err)
	} else {
		totalSuccessCount++
	}

	totalCount++
}

func getUserLifeCycleData(region int, serverDatas []ServerData, date time.Time) (userLifeCycleDatas []UserLifeCycleData) {

	var myServerDatas []ServerData

	//开始时间-6天 <= 用户最后充值时间 <= 结束时间-6天
	noChargeRegionValue := noChargeRegionValues[region]

	for _, serverData := range serverDatas {
		regionStartTime := int(serverData.OpenTime) + noChargeRegionValue[0]*86400
		var regionEndTime int
		if region == 20 {
			regionEndTime = int(time.Now().Unix())
		} else {
			regionEndTime = int(serverData.OpenTime) + noChargeRegionValue[1]*86400
		}

		rangeStartTime := regionStartTime - (6 * 86400)
		rangeEndTime := regionEndTime - (6 * 86400)

		userRoleDatas := getServerUserRoles(serverData.ServerId, regionEndTime)

		for _, userRoleData := range userRoleDatas {
			if userRoleData.LastChargeTime > 0 && userRoleData.LastChargeTime >= rangeStartTime && userRoleData.LastChargeTime <= rangeEndTime {
				serverData.UserCount++
			}
		}

		if serverData.UserCount > 0 {
			serverData.Type = 1
			myServerDatas = append(myServerDatas, serverData)
		}
	}

	//开始时间-3天 <= 用户最后登录时间 <= 结束时间-3天
	lostRegionValue := lostRegionValues[region]

	for _, serverData := range serverDatas {
		regionStartTime := int(serverData.OpenTime) + lostRegionValue[0]*86400
		var regionEndTime int
		if region == 20 {
			regionEndTime = int(time.Now().Unix())
		} else {
			regionEndTime = int(serverData.OpenTime) + lostRegionValue[1]*86400
		}

		rangeStartTime := regionStartTime - (3 * 86400)
		rangeEndTime := regionEndTime - (3 * 86400)

		userRoleDatas := getServerUserRoles(serverData.ServerId, regionEndTime)

		for _, userRoleData := range userRoleDatas {
			if userRoleData.LastLoginTime > 0 && userRoleData.LastLoginTime >= rangeStartTime && userRoleData.LastLoginTime <= rangeEndTime {
				serverData.UserCount++
			}
		}

		if serverData.UserCount > 0 {
			serverData.Type = 2
			myServerDatas = append(myServerDatas, serverData)
		}
	}

	for _, serverData := range myServerDatas {

		userLifeCycleData := new(UserLifeCycleData)
		userLifeCycleData.GameId = serverData.GameId
		userLifeCycleData.ServerId = serverData.ServerId
		userLifeCycleData.Region = region
		userLifeCycleData.Type = serverData.Type
		userLifeCycleData.UserCount = serverData.UserCount
		userLifeCycleData.Date = date.Format("2006-01")
		userLifeCycleData.DateTime = date.Unix()

		userLifeCycleDatas = append(userLifeCycleDatas, *userLifeCycleData)

	}

	return
}

func getServerUserRoles(serverId int64, endTime int) (userRoleDatas []UserRoleData) {
	querySql := fmt.Sprintf(`SELECT role_id,username FROM gc_user_role WHERE server_id = %d AND (add_time <= %d) AND role_id <> ''`, serverId, endTime)

	rows, err := DB2.Query(querySql)

	if err != nil {
		fmt.Println(err)
		return
	}

	userRoleData := new(UserRoleData)

	var myUserRoleDatas []UserRoleData
	for rows.Next() {
		rows.Scan(&userRoleData.RoleId, &userRoleData.Username)
		myUserRoleDatas = append(myUserRoleDatas, *userRoleData)
	}

	for _, userRoleData := range myUserRoleDatas {
		//最后登录时间
		querySql = fmt.Sprintf(`SELECT login_time last_login_time FROM gc_user_play_data WHERE username = '%s' AND login_time <= %d`, userRoleData.Username, endTime)
		row := DB.QueryRow(querySql)
		row.Scan(&userRoleData.LastLoginTime)

		//最后充值时间
		querySql = fmt.Sprintf(`SELECT MAX(create_time) last_charge_time FROM gc_order WHERE role_id = '%s' AND create_time <= %d`, userRoleData.RoleId, endTime)
		row = DB.QueryRow(querySql)
		row.Scan(&userRoleData.LastChargeTime)

		if userRoleData.LastLoginTime > 0 || userRoleData.LastChargeTime > 0 {
			userRoleDatas = append(userRoleDatas, userRoleData)
		}
	}

	return
}

func getServerDatas(startTime int64, endTime int64) (serverDatas []ServerData) {
	querySql := fmt.Sprintf(`SELECT game_id,game_server_id,open_time 
	FROM gc_game_server WHERE (open_time BETWEEN %d AND %d) AND game_server_id > 0`, startTime, endTime)

	if serverId > 0 {
		querySql = fmt.Sprintf(`SELECT game_id,game_server_id,open_time 
		FROM gc_game_server WHERE (open_time BETWEEN %d AND %d) AND game_server_id = %d`, startTime, endTime, serverId)
	}

	rows, err := DB.Query(querySql)

	if err != nil {
		fmt.Println(err)
		return
	}

	serverData := new(ServerData)

	for rows.Next() {
		rows.Scan(&serverData.GameId, &serverData.ServerId, &serverData.OpenTime)
		serverDatas = append(serverDatas, *serverData)
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

func resolveSecond(second int64) (time string) {

	minute := second / 60

	hour := minute / 60

	minute = minute % 60

	second = second - hour*3600 - minute*60

	time = fmt.Sprintf("%d:%d:%d", hour, minute, second)

	//fmt.Println(time)

	return
}
