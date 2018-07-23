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

    // info DB
    dbDriver := "mysql"
    dbUser := "root"
    dbPass := "root"
    dbName := "capitaldata"

    db := dbCreate(dbDriver, dbUser, dbPass, dbName)

	// Lecture dossier
	files, err := ioutil.ReadDir("./")
	if err != nil {
		log.Fatal(err)
	}
	var fileRegexp = regexp.MustCompile(`\d{4}\d{2}\d{2}_\d{2}\d{2}\d{2}_contactstream(.*).csv`)

    // boucle sur les fichiers
    for _, f := range files {
        if fileRegexp.MatchString(f.Name()){
            // add to treated_file
        	file, err := os.Open(f.Name())
            file_treated(f.Name())
        	if err != nil {
        		fmt.Println("Error:", err)
        		return
        	}
        	defer file.Close()

        	// option lecture CSV
        	reader := csv.NewReader(file)
        	reader.Comma = ','
        	currentLine := 0

        	// boucle sur Read (ligne par ligne)
        	for {
        		record, err := reader.Read()
        		if err == io.EOF {
        			break
        		} else if err != nil {
        			fmt.Println("Error:", err)
        		}

                // DEBUGGG
        		//fmt.Println("FILE:", f.Name(), "Line", currentLine, "content is", record, "and got", len(record), "fields\n")

        		// check data
        		var valid = 1
        		if (isset(record, 1) && len(record[0]) > 50){
        			fmt.Println("FILE:", f.Name(), "First Name", record[0], "on Line:", currentLine, "is too long (max:50)")
        			valid = 0
        		}
        		if (isset(record, 2) && len(record[1]) > 50){
        			fmt.Println("FILE:", f.Name(), "Last Name", record[1], "on Line:", currentLine, "is too long(max:50)")
        			valid = 0
        		}
        		if (isset(record, 3) && len(record[2]) > 100){
        			fmt.Println("FILE:", f.Name(), "Email", record[2], "on Line:", currentLine, "is too long(max:100)")
        			valid = 0
        		}
        		if (isset(record, 3)&& !EmailChecker(record[2])){
        			fmt.Println("FILE:", f.Name(), "Email", record[2], "on Line:", currentLine, "invalid format")
        			valid = 0
        		}

        		if valid == 1 {
        			// insert DB
        			Dbinsert, err := db.Prepare("INSERT INTO users(first_name, last_name, email) VALUES(?,?,?)")
        			if err != nil {
        				panic(err.Error())
        			}
                    if isset(record, 3){
                        Dbinsert.Exec(record[0], record[1], record[2])
                    } else {
                        fmt.Println("FILE:", f.Name(), "missing data on line:", currentLine)
                    }
        		}
        		currentLine += 1
        	}
        }
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


// fix a bug when trying to access for example: record[3] but array doesn't exist
// panic: runtime error: index out of range
func isset(arr []string, index int) bool {
    return (len(arr) > index)
}

func file_treated(treated string) {
    path := "./.file_treated_log.txt"
    var _, err = os.Stat(path)

	if os.IsNotExist(err) {
		var file, err = os.Create(path)
        if err != nil {
    		fmt.Println(err)
    	}
		defer file.Close()
	}
    var file, erro = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
    if erro != nil {
		fmt.Println(erro)
	}
	defer file.Close()

	_, err = file.WriteString(treated+"\n")
    if err != nil {
        fmt.Println(err)
    }
    err = file.Sync()
    if err != nil {
        fmt.Println(err)
    }
}
