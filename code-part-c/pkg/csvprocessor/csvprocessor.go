package csvprocessor

import (
	"encoding/csv"
	"math/rand"
	"os"
	"time"
)

const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

type csvFile struct {
	Headers     []string
	DataFileKey string
	Cols        int
	Rows        int
	Lines       [][]string
}

func NewCsvUpload(filepath string) (*csvFile, error) {
	//multipart.File fulfils the io.Reader interface which is an input to csv
	//reader
	file, err := os.Open(filepath)
	csvReader := csv.NewReader(file)
	lines, err := csvReader.ReadAll()
	//debug statement
	if err != nil {
		return nil, err
	}
	csvFile := &csvFile{Lines: lines}
	csvFile.SetHeaders()
	csvFile.SetRows()
	csvFile.GenerateDataFileKey()
	return csvFile, nil
}

func GenerateRandomDataFileKey() string {
	b := make([]byte, 10)
	var src = rand.NewSource(time.Now().UnixNano())
	for i, cache, remain := 9, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return string(b)
}

//Generate DataFileKey
func (csv *csvFile) GenerateDataFileKey() error {
	csv.DataFileKey = GenerateRandomDataFileKey()
	return nil
}

func (csv *csvFile) SetHeaders() {
	csv.Headers = csv.Lines[0]
	csv.Cols = len(csv.Lines[0])
}

func (csv *csvFile) SetRows() {
	csv.Rows = len(csv.Lines)
}

//GetHeaders: Returns the csv headers from the processed file.
func (csv *csvFile) GetHeaders() []string {
	return csv.Headers
}

func (csv *csvFile) GetData() [][]string {
	return csv.Lines
}

func (csv *csvFile) GetDataFileKey() string {
	return csv.DataFileKey
}
