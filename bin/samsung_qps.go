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
	ip           = flag.String("ip", "127.0.0.1", "ip")
	port         = flag.Int("port", 10000, "port")
	dbName       = flag.String("db", "test", "db")
	concurrent   = flag.Int("concurrent", 16, "concurrent for insert")
	batch        = flag.Int("batch", 64, "batch for insert")
	enableInsert = flag.Bool("insert", false, "enable_insert")
	insertTime   = flag.Int("insert_time", 6, "insert_time hour")
	dropTest     = flag.Bool("drop_test", true, "drop_test")
	dropDelay    = flag.Int("drop_delay", 45, "drop_delay")
	selectCount  = flag.Bool("select_count", false, "select_count before insert")
	shareFlag    = [4]bool{false}
	timer        = time.NewTimer(1 * time.Hour)
	letters      = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

var (
	sql1 = `create table if not exists t (
		id int not null,
		c1 char(32),
		c2 char(32),
		c3 char(64),
		c4 char(64),
		c5 char(64),
		c6 char(64),
		t1 timestamp not null default current_timestamp,
		t2 timestamp not null default current_timestamp,
		PRIMARY KEY (id, t1, c1, c2)
	)
	PARTITION BY RANGE (id) (
		PARTITION p0 VALUES LESS THAN (1000000),
		PARTITION p1 VALUES LESS THAN (2000000),
		PARTITION p2 VALUES LESS THAN (3000000),
		PARTITION p3 VALUES LESS THAN MAXVALUE
	)`
	sql2 = `insert into t(id, c1, c2, c3, c4, c5, c6) values`
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
		insertJob(db, 1)

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
	if *selectCount {
		fmt.Println("Select count for loading block cache...", time.Now())
		_, err := db.Exec(sql4)
		if err != nil {
			fmt.Println(err)
		}
	}
	fmt.Println("Insert job to partition", partNum, time.Now())
	local := *batch
	for i := 0; i < *concurrent; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			fmt.Println(err)
		}
		go func() {
			for {
				id := partNum*1000000 + rand.Intn(1000000-local-1)
				sql := sql2
				str32 := randSeq(32)
				str64 := randSeq(64)
				for i := 0; i < local; i++ {
					if i == local-1 {
						sql += fmt.Sprintf(" (%v, '%v', '%v', '%v', '%v', '%v', '%v')", id+i, str32, str32, str64, str64, str64, str64)
						break
					} else {
						sql += fmt.Sprintf(" (%v, '%v', '%v', '%v', '%v', '%v', '%v'),", id+i, str32, str32, str64, str64, str64, str64)
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

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
