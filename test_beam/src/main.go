package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

type FieldFormat struct {
	Offset int
	Length int
}

type Format struct {
	LgRecord int
	Fields   map[string]*FieldFormat
}

func main() {
	// format := read_format()
	// read_file(format)
	beam.Init()

}

func print_result(data map[string]string) string {
	res, err := json.Marshal(&data)
	if err != nil {
		panic("Cannot marshal format !")
	}
	return string(res)
}

func read_file(format Format) {
	f, err := os.Open("../read_file/data.txt")

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		result := parse_line(format, line)
		json_format := print_result(result)
		fmt.Println("%v", json_format)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func parse_line(format Format, line string) map[string]string {
	result := make(map[string]string)
	for k, v := range format.Fields {
		result[k] = line[v.Offset : v.Offset+v.Length]
	}
	return result
}

func read_format() Format {
	f, err := os.Open("../read_file/format.txt")

	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)
	// {
	// 	field_name: {
	// 		offset: "",
	// 		length: "",
	//      value: ""
	// 	}
	// }
	field_map := make(map[string]*FieldFormat)
	temp_lgRecord := 0

	for scanner.Scan() {
		line := scanner.Text()
		field_data := strings.Split(line, " ")
		if strings.HasPrefix(line, "LGRECORD") {
			lgRecord, err := strconv.Atoi(field_data[1])
			if err != nil {
				panic(fmt.Sprintf("erreur format LgRecord: %s", field_data[1]))
			}
			temp_lgRecord = lgRecord
		} else {
			if len(field_data) != 3 {
				panic(fmt.Sprintf("length field_data != 3: %v, line: %s", field_data, line))
			}
			offset, err := strconv.Atoi(field_data[1])
			if err != nil {
				panic(fmt.Sprintf("erreur format offset: %s", field_data[1]))
			}
			length, err := strconv.Atoi(field_data[2])
			if err != nil {
				panic(fmt.Sprintf("erreur format length: %s", field_data[2]))
			}
			field_map[field_data[0]] = &FieldFormat{Offset: offset, Length: length}
		}
	}

	format := Format{LgRecord: temp_lgRecord, Fields: field_map}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return format
}
