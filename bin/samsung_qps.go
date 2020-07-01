package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	ip                = flag.String("ip", "127.0.0.1", "ip")
	port              = flag.Int("port", 10000, "port")
	dbName            = flag.String("db", "test", "db")
	concurrent        = flag.Int("concurrent", 16, "concurrent for insert")
	batch             = flag.Int("batch", 64, "batch for insert")
	shareFlag         = [4]bool{false}
	timer             = time.NewTimer(1 * time.Second)
	enableInsert      = flag.Bool("insert", false, "enable_insert")
	insertTime        = flag.Int("insert_time", 6, "insert_time hour")
	dropTest          = flag.Bool("drop_test", true, "drop_test")
	dropDelay         = flag.Int("drop_delay", 45, "drop_delay")
	enableSelectCount = flag.Bool("enable_count", false, "enable Select Count")
)

var (
	sql1 = `create table if not exists t (
		id int not null,
		c1 char(64),
		c2 char(128),
		c3 char(64),
		c4 char(128),
		c5 char(64)
	)
	PARTITION BY RANGE (id) (
		PARTITION p0 VALUES LESS THAN (5000),
		PARTITION p1 VALUES LESS THAN (10000),
		PARTITION p2 VALUES LESS THAN (15000),
		PARTITION p3 VALUES LESS THAN MAXVALUE
	)`
	sql2 = `insert into t values`
	sql3 = `ALTER TABLE t DROP PARTITION `
	sql4 = `select count(*) from t`
)

func main() {
	flag.Parse()

	db := connect(*ip, *port, *dbName)

	if *enableInsert {
		fmt.Println("Ensure you have run \"drop table t\"")
		createTable(db)
		insertData(db)
		timer.Reset(time.Duration(*insertTime) * time.Hour)
		<-timer.C
		fmt.Println("Insert done")
	}

	if *dropTest {
		readInsertJob(db, 1)

		fmt.Printf("waiting to drop... (%dmin)\n", *dropDelay)
		timer.Reset(time.Duration(*dropDelay) * time.Minute)
		<-timer.C
		fmt.Println("start to drop", time.Now())
		dropPartition(db, 0)
		dropPartition(db, 2)
		dropPartition(db, 3)
		timer.Reset(40 * time.Minute)
		<-timer.C
		fmt.Println("All tests done")
	}
}

func connect(ip string, port int, db string) *sql.DB {
	dsn := fmt.Sprintf("root@tcp(%s:%d)/%s", ip, port, db)
	fmt.Println("connecting", dsn)
	dbConn, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Println(err)
	}
	return dbConn
}

func createTable(db *sql.DB) {
	rand.Seed(time.Now().Unix())
	_, err := db.Exec(sql1)
	if err != nil {
		fmt.Println(err)
	}
}

func dropPartition(db *sql.DB, num int) {
	sql := sql3 + fmt.Sprintf("p%v", num)
	_, err := db.Exec(sql)
	if err != nil {
		fmt.Println(err)
	}
}

func stopAll(db *sql.DB) {
	for i := 0; i < 4; i++ {
		shareFlag[i] = true
	}
}

func insertData(db *sql.DB) {
	insertJob(db, 0)
	insertJob(db, 1)
	insertJob(db, 2)
	insertJob(db, 3)
	fmt.Println("All insert job started...")
}

func insertJob(db *sql.DB, partNum int) {
	fmt.Println("Insert job to", partNum)
	local := *batch
	for i := 0; i < *concurrent; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			fmt.Println(err)
		}
		go func() {
			str := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+"
			for {
				id := partNum*5000 + rand.Intn(5000)
				sql := sql2
				for i := 0; i < local; i++ {
					if i == local-1 {
						sql += fmt.Sprintf(" (%v, '%v', '%v', '%v', '%v', '%v')", id+i, str, str, str, str, str)
						break
					} else {
						sql += fmt.Sprintf(" (%v, '%v', '%v', '%v', '%v', '%v'),", id+i, str, str, str, str, str)
					}
				}
				_, err := conn.ExecContext(context.Background(), sql)
				if err != nil {
					fmt.Println(err)
				}
				if shareFlag[partNum] {
					break
				}
			}
		}()
	}
}

func readInsertJob(db *sql.DB, partNum int) {
	if *enableSelectCount {
		fmt.Println("Select count(*) ...")
		db.Exec(sql4)
	}
	fmt.Println("Read and insert job to", partNum)

	for i := 0; i < *concurrent; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			fmt.Println(err)
		}
		go func() {
			str := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+"
			for {
				for i := partNum * 5000; i < (partNum+1)*5000; i++ {
					sql := sql2
					for i := 0; i < *batch; i++ {
						if i == *batch-1 {
							sql += fmt.Sprintf(" (%v, '%v', '%v', '%v', '%v', '%v')", i, str, str, str, str, str)
							break
						} else {
							sql += fmt.Sprintf(" (%v, '%v', '%v', '%v', '%v', '%v'),", i, str, str, str, str, str)
						}
					}
					_, err := conn.ExecContext(context.Background(), sql)
					if err != nil {
						fmt.Println(err)
					}
				}
			}
		}()
	}
}
