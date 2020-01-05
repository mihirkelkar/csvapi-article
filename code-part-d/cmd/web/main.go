package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	cpros "github.com/mihirkelkar/csvapi/pkg/csvprocessor"
	msql "github.com/mihirkelkar/csvapi/pkg/mysql"
)

type Application struct {
	ModelService msql.HoppyDB
}

type HeaderApiResponse struct {
	Payload []msql.Header `json:"payload"`
	Message string        `json:"message"`
}

type ContentApiResponse struct {
	Payload []msql.DataContent `json:"payload"`
	Message string             `json:"message"`
}

func HelloWorld(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "This is a hello from the golang web app")
}

//curl -v -X POST -F "myFile=@./noSession.csv" localhost:3000
func (app *Application) UploadFile(w http.ResponseWriter, r *http.Request) {
	fmt.Println("File Upload Endpoint Hit")
	// Parse our multipart form, 10 << 20 specifies a maximum
	// upload of 10 MB files.
	r.ParseMultipartForm(10 << 20)
	// it also returns the FileHeader so we can get the Filename,
	// the Header and the size of the file
	file, header, err := r.FormFile("myFile")
	if err != nil {
		fmt.Println("Error Retrieving the File")
		fmt.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer file.Close()

	//fit the new file struct and validate for errors.
	err = os.MkdirAll("./data/", 0755)
	if err != nil {
		fmt.Println("File could not be created")
		w.WriteHeader(http.StatusInternalServerError)
	}

	filename := strings.Replace(header.Filename, " ", "_", -1)

	f, err := os.OpenFile("./data/"+filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		fmt.Println("file could not be opened")
		w.WriteHeader(http.StatusInternalServerError)
	}

	io.Copy(f, file)
	if err != nil {
		fmt.Println("file could not be copied")
		w.WriteHeader(http.StatusInternalServerError)
	}
	defer f.Close()

	if err != nil {
		fmt.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	//Call the CSV processor
	csvFile, err := cpros.NewCsvUpload("./data/" + filename)
	if err != nil {
		fmt.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	//Insert the metadata into the database.
	datakey := csvFile.GetDataFileKey()
	lastID, err := app.ModelService.InsertMetaData(datakey)
	if err != nil {
		fmt.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	headers := csvFile.GetHeaders()
	datatypes := make([]string, 0)
	for range headers {
		datatypes = append(datatypes, "string")
	}

	//Insert headers into the database
	maxHeaders, err := app.ModelService.InsertHeaders(lastID, headers, datatypes)
	if err != nil {
		fmt.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	//enter the actual file content
	data := csvFile.GetData()
	err = app.ModelService.InsertData(maxHeaders, lastID, data)
	if err != nil {
		fmt.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	os.Remove("./data/" + filename)
	w.WriteHeader(http.StatusOK)
	fmt.Println(datakey)
	return
}

func (app *Application) GetHeaders(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	//userid will eventually be passed from the context.
	headers, err := app.ModelService.GetHeaders(id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(HeaderApiResponse{Payload: headers, Message: ""})
	return
}

func (app *Application) GetData(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	content, err := app.ModelService.GetData(id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	fmt.Println(content)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ContentApiResponse{Payload: content, Message: ""})
	return
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
	app := Application{ModelService: msql.NewHoppyDB(db)}

	//define the command line arguments.
	r := mux.NewRouter()
	r.HandleFunc("/", HelloWorld).Methods("GET")
	r.HandleFunc("/", app.UploadFile).Methods("POST")
	r.HandleFunc("/api/v1/{id:[a-z,0-9,A-Z,_,-]+}/headers", app.GetHeaders).Methods("GET")
	r.HandleFunc("/api/v1/{id:[a-z,0-9,A-Z,_,-]+}/data", app.GetData).Methods("GET")
	http.ListenAndServe(":3000", r)
}
