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
	ip            = flag.String("ip", "127.0.0.1", "ip")
	port          = flag.Int("port", 10000, "port")
	db_name       = flag.String("db", "test", "db")
	concurrent    = flag.Int("concurrent", 16, "concurrent for insert")
	batch         = flag.Int("batch", 64, "batch for insert")
	shared_flag   = [4]bool{false}
	timer         = time.NewTimer(1 * time.Second)
	enable_insert = flag.Bool("insert", false, "enable_insert")
	insert_time   = flag.Int("insert_time", 6, "insert_time hour")
	drop_test     = flag.Bool("drop_test", true, "drop_test")
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
)

func main() {
	flag.Parse()

	db := connect(*ip, *port, *db_name)

	if *enable_insert {
		fmt.Println("Ensure you have run \"drop table t\"")
		create_table(db)
		insert_data(db)
		timer.Reset(time.Duration(*insert_time) * time.Hour)
		<-timer.C
		fmt.Println("Insert done")
	}

	if *drop_test {
		fmt.Println("ready to insert... (10min)")
		timer.Reset(10 * time.Minute)
		<-timer.C

		*concurrent = 60
		*batch = 1
		shared_flag[0] = false
		insert_data_job(db, 1)

		fmt.Println("waiting to drop... (30min)")
		timer.Reset(30 * time.Minute)
		<-timer.C
		fmt.Println("start to drop", time.Now())
		drop_partition(db, 0)
		drop_partition(db, 2)
		drop_partition(db, 3)
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

func create_table(db *sql.DB) {
	rand.Seed(time.Now().Unix())
	_, err := db.Exec(sql1)
	if err != nil {
		fmt.Println(err)
	}
}

func drop_partition(db *sql.DB, num int) {
	sql := sql3 + fmt.Sprintf("p%v", num)
	_, err := db.Exec(sql)
	if err != nil {
		fmt.Println(err)
	}
}

func stop_all(db *sql.DB) {
	for i := 0; i < 4; i++ {
		shared_flag[i] = true
	}
}

func insert_data(db *sql.DB) {
	insert_data_job(db, 0)
	insert_data_job(db, 1)
	insert_data_job(db, 2)
	insert_data_job(db, 3)
	fmt.Println("All insert job started...")
}

func insert_data_job(db *sql.DB, part_num int) {
	fmt.Println("Insert job to", part_num)
	local_batch := *batch
	for i := 0; i < *concurrent; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			fmt.Println(err)
		}
		go func() {
			str := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+"
			for {
				id := part_num*5000 + rand.Intn(5000)
				insert_sql := sql2
				for i := 0; i < local_batch; i++ {
					if i == local_batch-1 {
						insert_sql += fmt.Sprintf(" (%v, '%v', '%v', '%v', '%v', '%v')", id+i, str, str, str, str, str)
						break
					} else {
						insert_sql += fmt.Sprintf(" (%v, '%v', '%v', '%v', '%v', '%v'),", id+i, str, str, str, str, str)
					}
				}
				_, err := conn.ExecContext(context.Background(), insert_sql)
				if err != nil {
					fmt.Println(err)
				}
				if shared_flag[part_num] {
					break
				}
			}
		}()
	}
}
