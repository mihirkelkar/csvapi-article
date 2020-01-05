package mysql

import (
	"database/sql"
	"fmt"
)

type HoppyDB interface {
	InsertMetaData(string) (int, error)
	InsertHeaders(int, []string, []string) (int, error)
	InsertData(int, int, [][]string) error
	GetHeaders(string) ([]Header, error)
	GetData(string) ([]DataContent, error)
}

type hoppyDB struct {
	DB *sql.DB
}

//Header : struct that holds a representation of headers
type Header struct {
	Position int
	Value    string
	DataType string
}

type DataContent struct {
	HeaderID int
	RowID    int
	Value    string
}

func NewHoppyDB(db *sql.DB) HoppyDB {
	return &hoppyDB{DB: db}
}

//Insert: Inserts the metadata about the file into the database.
func (s *hoppyDB) InsertMetaData(datafilekey string) (int, error) {

	stmt := `INSERT INTO  uid_datafile (datafilekey, user_id, private, filetype, created_at, updated_at)
           VALUES(?, 0, 0, "csv", UTC_TIMESTAMP(), UTC_TIMESTAMP())`

	result, err := s.DB.Exec(stmt, datafilekey)
	if err != nil {
		return 0, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	return int(id), nil
}

//InsertHeaders: Insert header information into the headers table. The code
func (s *hoppyDB) InsertHeaders(datafileId int, headers []string, datatype []string) (int, error) {
	stmt := `INSERT INTO uid_dataheaders (datafileid, headerid, header, datatype) VALUES (?, ?, ?, ?)`

	for index, val := range headers {
		_, err := s.DB.Exec(stmt, datafileId, index, val, datatype[index])
		if err != nil {
			return 0, err
		}
	}
	return len(headers), nil
}

func (s *hoppyDB) InsertData(maxHeaders int, datafileId int, data [][]string) error {
	stmt := `INSERT INTO uid_datacontent (datafileid, headerid, rowid, value)
					VALUES (?, ?, ?, ?)`

	for rowid, values := range data[1:] { //need to remove the header row
		for index, val := range values[:maxHeaders] { //only go until the max number of headers
			_, err := s.DB.Exec(stmt, datafileId, index, rowid, val)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *hoppyDB) GetHeaders(datafilekey string) ([]Header, error) {
	var datafileid int
	metadatasql := `SELECT datafileid FROM uid_datafile WHERE datafilekey='%s'`
	metadatasql = fmt.Sprintf(metadatasql, datafilekey)
	row := s.DB.QueryRow(metadatasql)
	err := row.Scan(&datafileid)

	stmt := `SELECT headerid, header, datatype from uid_dataheaders where datafileid=? order by headerid`
	headers := make([]Header, 0)
	rows, err := s.DB.Query(stmt, datafileid)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		header := new(Header)
		err := rows.Scan(&header.Position, &header.Value, &header.DataType)
		if err != nil {
			return nil, err
		}
		headers = append(headers, *header)
	}
	return headers, nil
}

func (s *hoppyDB) GetData(datafilekey string) ([]DataContent, error) {
	var maxrowid int
	var datafileid int
	metadatasql := `SELECT datafileid FROM uid_datafile WHERE datafilekey='%s'`
	metadatasql = fmt.Sprintf(metadatasql, datafilekey)
	row := s.DB.QueryRow(metadatasql)
	err := row.Scan(&datafileid)
	if err != nil {
		fmt.Println("could not find metadata")
		return nil, err
	}

	rowstmt := `SELECT MAX(rowid) FROM uid_datacontent where datafileid=?`

	stmt := `SELECT headerid, rowid, value from uid_datacontent where datafileid=? order by headerid LIMIT 100;`

	datalist := make([]DataContent, 0)

	row = s.DB.QueryRow(rowstmt, datafileid)

	err = row.Scan(&maxrowid)

	if err != nil {
		fmt.Println("could not find max rowid for data file")
		return nil, err
	}

	rows, err := s.DB.Query(stmt, datafileid)
	if err != nil {
		fmt.Println("Could not query the top 100 stmt")
		fmt.Println(err)
		return nil, err
	}

	for rows.Next() {
		data := new(DataContent)
		err := rows.Scan(&data.HeaderID, &data.RowID, &data.Value)
		if err != nil {
			return nil, err
		}
		datalist = append(datalist, *data)
	}
	return datalist, nil
}
