package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

func HelloWorld(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "This is a hello from the golang web app")
}

func GetDataBaseUrl() string {
	user := os.Getenv("USER")
	password := os.Getenv("PASS")
	url := os.Getenv("URL")
	database := os.Getenv("DB")

	return fmt.Sprintf(`%s:%s@tcp(%s:3306)/%s?parseTime=true`, user, password, url, database)
}

func main() {

	dsn := GetDataBaseUrl()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	// Open doesn't open a connection. Validate DSN data:
	err = db.Ping()
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	fmt.Println("Success")

	//define the command line arguments.
	r := mux.NewRouter()
	r.HandleFunc("/", HelloWorld).Methods("GET")
	http.ListenAndServe(":3000", r)
}
