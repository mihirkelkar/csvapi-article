package mysql

import "database/sql"

type HoppyDB interface {
	InsertMetaData(string) (int, error)
	InsertHeaders(int, []string, []string) (int, error)
	InsertData(int, int, [][]string) error
}

type hoppyDB struct {
	DB *sql.DB
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
