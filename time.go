package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	var now time.Time = time.Now()
	var year int = now.Year()
	var month = now.Month()

	fmt.Println(month, year)

	broken := "G# R#cks!"
	replacer := strings.NewReplacer("#", "o")
	fixed := replacer.Replace(broken)
	fmt.Println(fixed)

	var err error
	fmt.Print("Enter a grade: ")
	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')

	if err != nil {
		log.Fatal(err)
	}
	input = strings.TrimSpace(input)
	grade, err := strconv.ParseFloat(input, 64)
	var status string

	if grade < 60 {
		status = "failing"
	} else {
		status = "passing"
	}
	fmt.Println("A grade of", grade, "is", status)
	fmt.Println(input)
}
