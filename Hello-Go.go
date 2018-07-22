package main

import (
	"fmt"
	"encoding/csv"
	"io"
	"regexp"
	"io/ioutil"
	"log"
	"os"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func main(){

	// Ouverture fichier
	files, err := ioutil.ReadDir("./")
	if err != nil {
		log.Fatal(err)
	}
	var fileRegexp = regexp.MustCompile(`\d{4}\d{2}\d{2}_\d{2}\d{2}\d{2}_contactstream(.*).csv`)
	for _, f := range files {
		if fileRegexp.MatchString(f.Name()){
			fmt.Println(f.Name())
		}
	}
	file, err := os.Open("20180101_132200_contactstream2.csv")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	// fermer le fichier a la fin
	defer file.Close()

	// option lecture CSV
	reader := csv.NewReader(file)
	reader.Comma = ','
	currentLine := 0

	// info DB
	dbDriver := "mysql"
	dbUser := "root"
	dbPass := "root"
	dbName := "capitaldata"

	db := dbCreate(dbDriver, dbUser, dbPass, dbName)


	// boucle sur Read (ligne par ligne)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error:", err)
		}

		// DEBUGGG
		// fmt.Println("Line", currentLine, "content is", record, "and got", len(record), "fields\n")

		// check data
		var valid = 1
		if (len(record[0]) > 50){
			fmt.Println("Error: First Name", record[0],  "on Line:", currentLine, "is too long (max:50)")
			valid = 0
		}
		if (len(record[1]) > 50){
			fmt.Println("Error: Last Name", record[1],  "on Line:", currentLine, "is too long(max:50)")
			valid = 0
		}
		if (len(record[2]) > 100){
			fmt.Println("Error: Email", record[2],  "on Line:", currentLine, "is too long(max:100)")
			valid = 0
		}
		if (!EmailChecker(record[2])){
			fmt.Println("Error: Email", record[2],  "on Line:", currentLine, "invalid format")
			valid = 0
		}

		if valid == 1 {
			// insert DB
			Dbinsert, err := db.Prepare("INSERT INTO users(first_name, last_name, email) VALUES(?,?,?)")
			if err != nil {
				panic(err.Error())
			}
			Dbinsert.Exec(record[0], record[1], record[2])
		}
		currentLine += 1
	}
}

func EmailChecker(email string) bool {
	var emailRegexp = regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")
	if !emailRegexp.MatchString(email){
		return false
	}
	return true
}
func dbCreate(dbDriver string, dbUser string, dbPass string, dbName string) *sql.DB {
	db, err := sql.Open(dbDriver, dbUser+":"+dbPass+"@/")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// creation db
	_,err = db.Exec("CREATE DATABASE IF NOT EXISTS "+dbName)
	if err != nil {
		panic(err)
	}
	db, err = sql.Open(dbDriver, dbUser+":"+dbPass+"@/"+dbName+"?parseTime=true")
	if err != nil {
		panic(err)
	}
	_,err = db.Exec("USE "+dbName)
	if err != nil {
		panic(err)
	}

	// creation table
	_,err = db.Exec(`CREATE TABLE IF NOT EXISTS users (
		id INT UNSIGNED NOT NULL AUTO_INCREMENT,
		first_name VARCHAR(50) NULL,
		last_name VARCHAR(50) NULL,
		email VARCHAR(100) NULL,
		birthdate DATETIME NULL,
		PRIMARY KEY (id)
	)`)
	if err != nil {
		panic(err)
	}
	return db
}
