package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type FieldFormat struct {
	Offset int
	Length int
}

type Format struct {
	LgRecord int
	Fields   map[string]*FieldFormat
}

type LineBatch struct {
	id    int
	lines *[]string
}

type Result struct {
	id   int
	data *[]map[string]string
}

func main() {
	// ouverture de fichier format
	formatFile, err := os.Open("../format.txt")

	if err != nil {
		log.Fatal(err)
	}

	// fermeture du fichier format à la fin
	defer formatFile.Close()

	// ouverture du fichier data
	data, err := os.Open("../data2.txt")

	if err != nil {
		log.Fatal(err)
	}

	// fermeture du fichier data à la fin
	defer data.Close()

	// init format
	format := read_format(formatFile)

	// read data -> line_channel -> workers
	line_channel := make(chan *LineBatch)

	// workers -> result_channel -> printer
	result_channel := make(chan *Result)

	// printer -> end_printer_channel -> main
	end_printer_channel := make(chan bool)

	// taille d'un tableau de lignes
	bufferSize := 100

	// nombre de workers
	nb_workers := 10

	// init printer
	go run_printer(result_channel, nb_workers, end_printer_channel)

	// init workers
	for i := 0; i < nb_workers; i++ {
		go run_parser(format, line_channel, result_channel)
	}

	// lecture du fichier data
	read_file(data, format, line_channel, bufferSize, nb_workers)

	// attente fin du printer
	_ = <-end_printer_channel

	fmt.Println("end main")
}

func run_printer(result_channel chan *Result, nb_workers int, end_printer_channel chan bool) {
	// on print les formats finaux
	nbWorkerStopped := 0
	receivedResults := make([]*Result, 0)

	for nbWorkerStopped != nb_workers {
		result := <-result_channel
		if result == nil {
			nbWorkerStopped++
		} else {
			receivedResults = append(receivedResults, result)
		}
	}

	// tous les résultats ont été récupérés, on peut trier
	sort.SliceStable(receivedResults, func(i int, j int) bool {
		return (*receivedResults[i]).id < (*receivedResults[j]).id
	})

	// pour chaque chunk
	for _, result := range receivedResults {
		// pour chaque (ligne formatée) du chunk
		for _, data := range *result.data {
			res := print_result(&data)
			fmt.Println(res)
		}
	}
	end_printer_channel <- true
}

func print_result(result *map[string]string) string {
	res, err := json.Marshal(result)

	if err != nil {
		panic("Cannot marshal format !")
	}
	return string(res)
}

func run_parser(format Format, line_channel chan *LineBatch, result_channel chan *Result) {
	stop := false
	for !stop {
		lineBatch := <-line_channel
		if lineBatch == nil {
			// il n'y a plus de lignes à traiter, on s'arrête
			stop = true
			// on signale au printer qu'on s'arrête
			result_channel <- nil
		} else {
			// on parse
			datas := make([]map[string]string, 0)
			for _, line := range *(*lineBatch).lines {
				data := parse_line(format, &line)
				datas = append(datas, data)
			}

			id := (*lineBatch).id
			result := Result{
				id:   id,
				data: &datas,
			}

			// on met le format dans la sortie
			result_channel <- &result
		}
	}
}

func read_file(dataFile *os.File, format Format, line_channel chan *LineBatch, bufferSize int, nb_workers int) {
	// ouverture de fichier data
	scanner := bufio.NewScanner(dataFile)

	bufferLine := make([]string, 0)
	batchId := 0

	emptyLine := regexp.MustCompile(`^\s*$`)

	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		if !emptyLine.MatchString(line) {
			// on met la ligne dans le buffer
			bufferLine = append(bufferLine, line)
		}
		if len(bufferLine) == bufferSize {
			// copie de l'array car on va reinit
			lines := bufferLine
			// on associe l'id du batch aux lignes
			batch := LineBatch{
				id:    batchId,
				lines: &lines,
			}
			// on met les lignes dans le channel
			line_channel <- &batch
			// on reinit
			bufferLine = make([]string, 0)
			batchId++
		}
	}

	// on le refait une dernière fois pour les dernières lignes
	if len(bufferLine) > 0 {
		// on associe l'id du batch aux lignes
		batch := LineBatch{
			id:    batchId,
			lines: &bufferLine,
		}
		// on met les lignes dans le channel
		line_channel <- &batch
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// on signale la fin de la lecture aux workers
	for i := 0; i < nb_workers; i++ {
		line_channel <- nil
	}
}

func parse_line(format Format, line *string) map[string]string {
	// pour chaque champ, récupérer la data en fonction de l'offset et longueur du champ
	result := make(map[string]string)
	for k, v := range format.Fields {
		result[k] = (*line)[v.Offset : v.Offset+v.Length]
	}
	return result
}

func read_format(formatFile *os.File) Format {

	scanner := bufio.NewScanner(formatFile)
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
			// longueur d'une ligne
			lgRecord, err := strconv.Atoi(field_data[1])
			if err != nil {
				panic(fmt.Sprintf("erreur format LgRecord: %s", field_data[1]))
			}
			temp_lgRecord = lgRecord
		} else {
			// on récupère offset et longueur pour chaque champ
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
