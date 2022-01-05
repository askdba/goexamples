package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
)

var insUserQuery = "INSERT into user (team_id,name,email) values (?,?,?)"
var insTeamQuery = "insert into team (team_id,user_id, name) values (?,?,?);"
var selQuery = "select email from user where user_id=?"
var randUserQuery = "select user_id from user order by rand() limit 1;"
var rowsPer = 50000

func main() {

	numThreads := flag.Int("num_threads", 10, "number of concurrent readers/writers")
	mode := flag.String("mode", "read", "read/write")

	flag.Parse()
	if *mode != "read" && *mode != "write" {
		fmt.Printf("Unknown mode: %v\n", *mode)
		os.Exit(1)
	}

	if *numThreads < 1 {
		fmt.Println("numThreads must be >= 1")
		os.Exit(1)
	}
	dsn, ok := os.LookupEnv("DATABASE_URL")
	if !ok {
		fmt.Println("DATABASE_URL not specified")
		os.Exit(2)
	}

	for i := 0; i < *numThreads; i++ {
		switch *mode {
		case "write":
			fmt.Printf("Launching insert loop %d\n", i)
			go insertLoop(dsn, i)
		case "read":
			fmt.Printf("Launching select loop %d\n", i)
			go selectLoop(dsn, i)
		}
	}

	done := make(chan struct{})

	go func() {
		c := make(chan os.Signal, 1) // we need to reserve to buffer size 1, so the notifier are not blocked
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)

		<-c
		close(done)
	}()

	<-done
	fmt.Println("i'm done")
}

func insertLoop(dsn string, index int) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("error connecting to db: %v\n", dsn)
		return err
	}

	if err := db.Ping(); err != nil {
		return err
	}

	// get the actual Team ID
	teamID := ""
	teamName := ""
	if rand.Int63n(5) == 0 { // create a new team
		teamID = randData(10)
		teamName = randData(10)
	} else { // use an old team
		rows, err := db.Query("select team_id, name from team order by rand() limit 1");
		if err != nil {
			fmt.Printf("Error selecting random user id: %v\n", err)
			return err
		}
		if rows.Next() {
			err := rows.Scan(&teamID, &teamName)
			if err != nil {
				return err
			}
		} else {
			teamID = randData(10)
			teamName = randData(10)
		}
	}

	// first insert a user record
	q, err := db.Prepare(insUserQuery)
	if err != nil {
		fmt.Printf("Error preparing query: %v\n", insUserQuery)
		return err
	}

	seed := time.Now().Nanosecond()
	email := randData(18) + "_" + strconv.Itoa(seed) + "@mydomain.com"
	name := randData(25)

	res, err := q.Exec(teamID,name,email)
	if err != nil {
		fmt.Printf("Error inserting user data: %v\n", err)
		return err
	}

	userID, err := res.LastInsertId()
	if err != nil {
		return err
	}

	// allow enough time for all threads to finish inserting 1 user
	time.Sleep(time.Second)

	q, err = db.Prepare(insTeamQuery)
	if err != nil {
		fmt.Printf("error preparing query: %v\n", insTeamQuery)
		return err
	}

	if _, err := q.Exec(teamID, userID, teamName)
	err != nil {
		fmt.Printf("Error inserting team data: %v\n", err)
		return err
	}
	return nil
}

func selectLoop(dsn string, index int) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("error connecting to db: %v\n", dsn)
		return err
	}

	if err := db.Ping(); err != nil {
		return err
	}

	q, err := db.Prepare(selQuery)
	if err != nil {
		fmt.Printf("error preparing query: %v\n", selQuery)
		return err
	}
	for {
		// generate random id in range
		id := index*rowsPer + rand.Intn(rowsPer)
		fmt.Printf("selecting id=%v\n", id)
		if _, err := q.Exec(id); err != nil {
			fmt.Printf("error selecting data: %v", err)
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randData(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}
