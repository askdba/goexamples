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

	// first insert a user record

	q, err := db.Prepare(insUserQuery)
	if err != nil {
		fmt.Printf("Error preparing query: %v\n", insUserQuery)
		return err
	}

	email := randData(18) + "@mydomain.com"
	teamID := randData(11)
	name := randData(25)

	if _, err := q.Exec(teamID,name,email);
		err != nil {
		fmt.Printf("Error inserting user data: %v\n", err)
		return err
	}

	// allow enough time for all threads to finish inserting 1 user
	time.Sleep(time.Second)

	q, err = db.Prepare(insTeamQuery)
	if err != nil {
		fmt.Printf("error preparing query: %v\n", insTeamQuery)
		return err
	}

	q, err = db.Prepare(randUserQuery)
	if err != nil {
		fmt.Printf("Error preparing query: %v\n", randUserQuery)
		return err
	}
	//for i := index * rowsPer; i < (index+1)*rowsPer; i++ {
		// generate a random id between 1 & 20
		// TODO: this should be same as numThreads
		// teamID = random 11-char string
	time.Sleep(time.Second)
		randusrId,err := q.Query(randUserQuery);
		fmt.Printf("User id=%v\n", randusrId)
		if err != nil {
			fmt.Printf("Error selecting random user id: %v\n", err)
			return err
		}
    for randusrId.Next() {
		teamID := randData(11)
		fmt.Printf("Inserting new team for team_id=%v\n", teamID)

		if _, err := q.Exec(teamID,randusrId,name)
		err != nil {
			fmt.Printf("Error inserting team data: %v\n", err)
			return err
		}
		time.Sleep(10 * time.Millisecond)
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
