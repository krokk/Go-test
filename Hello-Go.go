package main

import (
	"fmt"
    "encoding/csv"
	"io"
    "log"
	"os"
    "database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func main(){

    // Ouverture fichier
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

    // connection sql
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
            if (len(record[0]) > 50){
                fmt.Println("Error: First Name", record[0],  "on Line:", currentLine, "is too long")
            }
            if (len(record[1]) > 50){
                fmt.Println("Error: Last Name", record[1],  "on Line:", currentLine, "is too long")
            }
            if (len(record[2]) > 100){
                fmt.Println("Error: Email", record[2],  "on Line:", currentLine, "is too long")
            }
            if err != nil {
            	log.Fatal(err)
                break
            }
            // insert DB
            Dbinsert, err := db.Prepare("INSERT INTO users(first_name, last_name, email) VALUES(?,?,?)")
            if err != nil {
                panic(err.Error())
            }
            Dbinsert.Exec(record[0], record[1], record[2])
    
		currentLine += 1
	}
}
