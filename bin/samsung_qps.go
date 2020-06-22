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
	ip          = flag.String("ip", "127.0.0.1", "ip")
	port        = flag.Int("port", 10000, "port")
	db_name     = flag.String("db", "test", "db")
	shared_flag = [5]bool{false}
)

var (
	sql1 = `create table if not exists t (
		id int not null,
		c1 char(64),
		c2 char(64),
		c3 char(64),
		c4 char(64),
		c5 char(64)
	)
	PARTITION BY RANGE (id) (
		PARTITION p0 VALUES LESS THAN (10000),
		PARTITION p1 VALUES LESS THAN (20000),
		PARTITION p2 VALUES LESS THAN (30000),
		PARTITION p3 VALUES LESS THAN (40000),
		PARTITION p4 VALUES LESS THAN MAXVALUE
	)`
	sql2 = `insert into t values`
	sql3 = `ALTER TABLE t DROP PARTITION `
)

func main() {
	flag.Parse()

	db := connect(*ip, *port, *db_name)
	create_table(db)
	insert_data(db)

	fmt.Println("1. Which partition do you want to drop? [0~4]:")
	var num int
	fmt.Scan(&num)
	drop_partition(db, num)
	fmt.Println("2. Which partition do you want to drop? [0~4]:")
	fmt.Scan(&num)
	drop_partition(db, num)
	fmt.Println("Enter to exit")
	fmt.Scanln()
}

func connect(ip string, port int, db string) *sql.DB {
	fmt.Println("Ensure you have run \"drop table t\"")
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
	shared_flag[num] = true
	sql := sql3 + fmt.Sprintf("p%v", num)
	_, err := db.Exec(sql)
	if err != nil {
		fmt.Println(err)
	}
}

func insert_data(db *sql.DB) {
	insert_data_job(db, 8, 2, 0)
	insert_data_job(db, 8, 2, 1)
	insert_data_job(db, 8, 2, 2)
	insert_data_job(db, 8, 2, 3)
	insert_data_job(db, 8, 2, 4)
	fmt.Println("All insert job started...")
}

func insert_data_job(db *sql.DB, concurrent int, batch int, part_num int) {
	for i := 0; i < concurrent; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			fmt.Println(err)
		}
		go func() {
			str := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+"
			for {
				id := part_num*10000 + rand.Intn(10000)
				insert_sql := sql2
				for i := 0; i < batch; i++ {
					if i == batch-1 {
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
					fmt.Println("stop insert into partition", part_num)
					break
				}
			}
		}()
	}
}
