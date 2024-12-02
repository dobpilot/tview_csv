// main.go
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"net/http"
	_ "net/http/pprof"

	"github.com/segmentio/fasthash/fnv1a"
	"gopkg.in/alecthomas/kingpin.v2"
)

type tline struct {
	count, summ, max, avg int
	keys                  []string
	id                    string
}

var (
	aggr, group, format string
	files, groupField   []string
	profile             bool
)

func processFile(filePath string, wg *sync.WaitGroup, results chan map[string]tline) {

	defer wg.Done()

	var isMultiLine bool
	var line string
	var lines map[string]tline

	isMultiLine = false
	line = ""

	//fmt.Printf("FilePath: %s\n", filePath)

	//ata, err := os.ReadFile(filePath)

	file, err := os.Open(filePath) // Открываем файл

	if err != nil {
		fmt.Printf("Ошибка чтения файла %s: %v\n", filePath, err)
		return
	}
	defer file.Close() // Закрываем файл в конце функции

	scanner := bufio.NewScanner(file) // Создаем сканер для чтения файла построчно

	linedata := &tline{
		keys:  []string{},
		count: 1,
	}

	lines = make(map[string]tline)

	for scanner.Scan() {

		var intVal int

		if isMultiLine {
			line = line + "\n" + scanner.Text()
		} else {
			line = scanner.Text()
			line = strings.Replace(line, "\ufeff", "", 1)
		}

		// Используем форматирование из format.go
		//formatter := &formatter1C{}
		formatter := new(formatter1C)

		parsedData, err := formatter.Format(line)

		if err != nil {
			isMultiLine = true
			continue
		}

		if isMultiLine {
			isMultiLine = false
		}

		if len(groupField) > 0 {
			for _, field := range groupField {
				linedata.keys = append(linedata.keys, parsedData[field])
			}
		} else {
			linedata.keys = append(linedata.keys, line)
		}

		if aggr != "" {
			intVal, _ = strconv.Atoi(parsedData[aggr])
		}

		key := getHash(strings.Join(linedata.keys, "-"))
		linedata.id = key

		if _, exists := lines[key]; exists {
			// Получаем текущее значение строки по ключу
			currentLine := lines[key]
			currentLine.count++
			currentLine.max = int(math.Max(float64(intVal), float64(currentLine.max)))
			currentLine.summ += intVal
			// Обновляем значение в мапе
			lines[key] = currentLine
		} else {
			// Инициализируем новую запись в мапе, если ключ не существует
			lines[key] = tline{
				keys:  linedata.keys,
				count: 1,
				max:   intVal,
				summ:  intVal,
			}
		}

		linedata = &tline{
			keys:  []string{},
			count: 1,
		}
	}

	results <- lines
}

func init() {

	kingpin.Flag("group", "Имена свойств для по которым нужно группировать").Short('g').Default("event").StringVar(&group)
	kingpin.Flag("aggregate", "Имя свойства для агрегации").Short('a').Default("duration").StringVar(&aggr)
	kingpin.Flag("format", "Формат вывода: csv или json").Short('o').Default("csv").StringVar(&format)
	kingpin.Flag("profile", "Включить профилирование").Short('p').Default("0").BoolVar(&profile)

	kingpin.Arg("files", "Файлы тех журнала *.log").Required().StringsVar(&files)

	runtime.SetMutexProfileFraction(5)
}

func (line tline) MarshalJSON() ([]byte, error) {
	var lineMap = make(map[string]any)

	lineMap["Count"] = line.count
	lineMap["Avg"] = line.avg
	lineMap["Max"] = line.max
	lineMap["Summ"] = line.summ

	if len(groupField) > 0 {
		for key, value := range groupField {
			lineMap[value] = line.keys[key]
		}
	}

	return json.Marshal(&lineMap)
}

func exportJSON(w io.Writer, results map[string]tline) {
	rows := make([]tline, 0, len(results))

	for _, value := range results {
		rows = append(rows, value)
	}

	json.NewEncoder(w).Encode(rows)
}

func exportCSV(w io.Writer, results map[string]tline) {

	fmt.Fprintf(w, "Count,Summ,Max") // Записываем заголовки

	if len(groupField) > 0 {
		for _, field := range groupField {
			fmt.Print(",", field)
		}
	}
	fmt.Fprintln(w)

	for _, value := range results {
		fmt.Fprintf(w, "%d,%d,%d", value.count, value.summ, value.max)

		if len(groupField) > 0 {
			for key, _ := range groupField {
				if strings.Contains(value.keys[key], "\n") {
					fmt.Fprint(w, ",'", value.keys[key], "'")
				} else {
					fmt.Fprint(w, ",", value.keys[key])
				}
			}
		}

		fmt.Fprintln(w)
	}
}

func main() {
	kingpin.Version("0.0.1")
	kingpin.Parse()

	if profile {
		go func() {
			log.Println(http.ListenAndServe(":8080", nil))
		}()
	}

	var filesTJ []string

	for _, mask := range files {
		findFiles, err := filepath.Glob(mask)
		if err != nil { // Проверка на ошибку
			log.Printf("Ошибка при поиске файлов: %v\n", err)
			continue
		}
		filesTJ = append(filesTJ, findFiles...)
	}

	if len(filesTJ) == 0 {
		log.Fatal("Нет файлов для анализа")
		os.Exit(1)
	}

	groupField = strings.Split(group, ",")

	results := make(chan map[string]tline, len(filesTJ))

	var wg sync.WaitGroup

	for _, filePath := range filesTJ {
		wg.Add(1)
		go processFile(filePath, &wg, results)
	}

	wg.Wait()

	close(results)

	allResults := make(map[string]tline)

	for elem := range results {
		for key, value := range elem {

			if _, exists := allResults[key]; exists {
				currentLine := allResults[key]
				currentLine.count = currentLine.count + value.count
				currentLine.max = int(math.Max(float64(value.max), float64(currentLine.max)))
				currentLine.summ += value.summ
				// Обновляем значение в мапе
				allResults[key] = currentLine
			} else {
				allResults[key] = value
			}
		}
	}

	if format == "csv" {
		// Сохраняем allResults в CSV в stdout
		exportCSV(os.Stdout, allResults)
	} else if format == "json" {
		exportJSON(os.Stdout, allResults)
	}
}

func getHash(inStr string) string {
	//Sum := md5.Sum([]byte(inStr))
	//return fmt.Sprintf("%x", Sum)
	Sum := fnv1a.HashString64(inStr)
	return fmt.Sprintf("%x", Sum)
}
