// main.go
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
	count, summ, max, min, avg uint64
	keys                       []string
	id                         uint64
}

var (
	aggr, group, format string
	files, groupField   []string
	filterStrings       []string
	profile             bool
)

func Min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func Max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func processFile(filePath string, wg *sync.WaitGroup, results chan map[uint64]tline, filter *Filter) {

	defer wg.Done()

	var isMultiLine bool
	var line strings.Builder
	lines := make(map[uint64]tline)

	isMultiLine = false

	//fmt.Printf("FilePath: %s\n", filePath)

	//ata, err := os.ReadFile(filePath)

	file, err := os.Open(filePath) // Открываем файл

	if err != nil {
		fmt.Printf("Ошибка чтения файла %s: %v\n", filePath, err)
		return
	}
	defer file.Close() // Закрываем файл в конце функции

	//scanner := bufio.NewScanner(file) // Создаем сканер для чтения файла построчно
	reader := bufio.NewReader(file)

	var intVal uint64

	countOfTerm := 0

	// Канал для передачи строк в горутины обработки
	linesToProcess := make(chan string, 100) // Буферизованный канал

	// Пул горутин для асинхронного форматирования и добавления в map
	// Удаляем пул, оставляем одну горутину
	go func() {
		for lineStr := range linesToProcess {
			formatter := &formatter1C{}
			parsedData, err := formatter.Format(lineStr)

			// TODO: Fixme
			if err != nil {
				continue
			}

			// Применение фильтра
			if !filter.Apply(parsedData) {
				continue // Пропускаем запись, не соответствующую фильтру
			}

			linedata := tline{ // Используем значение, а не указатель
				keys:  []string{},
				count: 1,
			}

			if len(groupField) > 0 {
				for _, field := range groupField {
					linedata.keys = append(linedata.keys, parsedData[field])
				}
			} else {
				for k := range parsedData {
					linedata.keys = append(linedata.keys, parsedData[k])
				}
			}

			if aggr != "" {
				intVal, _ = strconv.ParseUint(parsedData[aggr], 10, 64)
			}

			key := getHash(strings.Join(linedata.keys, "-"))
			linedata.id = key
			linedata.max = intVal
			linedata.min = intVal
			linedata.summ = intVal
			linedata.avg = intVal

			// Асинхронное добавление в map (без мьютекса, т.к. один обработчик)
			if currentLine, exists := lines[key]; exists {
				// Получаем текущее значение строки по ключу
				currentLine.count++
				currentLine.max = Max(intVal, currentLine.max)
				currentLine.min = Min(intVal, currentLine.min)
				currentLine.summ += intVal
				currentLine.avg = currentLine.summ / currentLine.count
				// Обновляем значение в мапе
				lines[key] = currentLine
			} else {
				// Инициализируем новую запись в мапе, если ключ не существует
				lines[key] = linedata
			}
		}
	}()

	// Основной цикл чтения файла
	for {
		data, _, err := reader.ReadLine()

		if err != nil {
			break // Конец файла или ошибка чтения
		}

		if isMultiLine {
			line.Write(data)
			//line = fmt.Sprintf("%s\n%s", line, data)
		} else {
			line.Reset()

			buf := strings.Replace(string(data), "\ufeff", "", 1)

			line.WriteString(buf)
		}

		// Подсчет кавычек с использованием новой функции
		countOfTerm = countOfTerm + getTermCount(data)

		if countOfTerm > 0 && countOfTerm%2 > 0 {
			isMultiLine = true
			continue
		}

		if isMultiLine {
			isMultiLine = false
		}

		countOfTerm = 0

		// Отправляем полную строку на обработку в горутину
		linesToProcess <- line.String()
	}

	close(linesToProcess) // Закрываем канал после завершения чтения
	//processWg.Wait()      // Ждем завершения работы всех горутин обработки (удаляем для одного обработчика)

	results <- lines // Отправляем финальные результаты
}

// getTermCount подсчитывает количество символов ' и " в срезе байт.
func getTermCount(data []byte) int {
	count := 0
	for _, b := range data {
		if b == '\'' || b == '"' {
			count++
		}
	}
	return count
}

func init() {
	kingpin.Flag("group", "Имена свойств для по которым нужно группировать").Short('g').Default("event").StringVar(&group)
	kingpin.Flag("aggregate", "Имя свойства для агрегации").Short('a').Default("duration").StringVar(&aggr)
	kingpin.Flag("format", "Формат вывода: csv или json").Short('o').Default("csv").StringVar(&format)
	kingpin.Flag("filter", "Фильтр по ключу и значению key=value (key=\"value\") (можно указывать несколько)").Short('f').StringsVar(&filterStrings)
	kingpin.Flag("profile", "Включить профилирование").Short('p').Default("0").BoolVar(&profile)
	kingpin.Arg("files", "Файлы тех журнала *.log").Required().StringsVar(&files)
	runtime.SetMutexProfileFraction(5)
}

func (line tline) MarshalJSON() ([]byte, error) {
	var lineMap = make(map[string]any)

	lineMap["Count"] = line.count
	lineMap["Avg"] = line.avg
	lineMap["Max"] = line.max
	lineMap["Min"] = line.min
	lineMap["Summ"] = line.summ

	if len(groupField) > 0 {
		for key, value := range groupField {
			lineMap[value] = line.keys[key]
		}
	}

	return json.Marshal(&lineMap)
}

// exportJSON теперь принимает срез tline
func exportJSON(w io.Writer, results []tline) {
	json.NewEncoder(w).Encode(results)
}

// exportCSV теперь принимает срез tline
func exportCSV(w io.Writer, results []tline) {
	fmt.Fprintf(w, "Count,Summ,Max,Min,Avg") // Записываем заголовки
	if len(groupField) > 0 {
		for _, field := range groupField {
			fmt.Print(",", field)
		}
	}
	fmt.Fprintln(w)
	for _, value := range results {
		fmt.Fprintf(w, "%d,%d,%d,%d,%d", value.count, value.summ, value.max, value.min, value.avg)
		if len(groupField) > 0 {
			for key, _ := range groupField {
				if strings.Contains(value.keys[key], "\n") {
					fmt.Fprint(w, ",", strconv.Quote(value.keys[key]))
				} else {
					fmt.Fprint(w, ",", value.keys[key])
				}
			}
		}
		fmt.Fprintln(w)
	}
}

func main() {

	kingpin.Version("0.0.4")
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

	// Парсинг фильтров
	filter := &Filter{conditions: []FilterCondition{}}
	for _, filterStr := range filterStrings {
		f, err := ParseFilter(filterStr)
		if err != nil {
			log.Printf("Ошибка парсинга фильтра '%s': %v\n", filterStr, err)
			continue
		}
		filter.conditions = append(filter.conditions, f.conditions...)
	}

	results := make(chan map[uint64]tline, len(filesTJ))

	var wg sync.WaitGroup // Для ожидания processFile goroutines

	// Дополнительная WaitGroup для ожидания горутины обработки results
	//var processResultsWg sync.WaitGroup
	//processResultsWg.Add(1)

	allResults := make(map[uint64]tline)

	// Горутина для обработки результатов из канала results
	//}()

	for _, filePath := range filesTJ {
		wg.Add(1)
		go processFile(filePath, &wg, results, filter)
	}

	wg.Wait()      // Ждем завершения всех processFile goroutines
	close(results) // Закрываем канал results после того, как все processFile завершились
	//processResultsWg.Wait() // Ждем завершения горутины обработки results

	for elem := range results {
		for key, value := range elem {
			if _, exists := allResults[key]; exists {
				currentLine := allResults[key]
				currentLine.count += value.count
				currentLine.max = Max(value.max, currentLine.max)
				currentLine.min = Min(value.min, currentLine.min)
				currentLine.summ += value.summ
				currentLine.avg = currentLine.summ / currentLine.count
				// Обновляем значение в мапе
				allResults[key] = currentLine
			} else {
				allResults[key] = value
			}
		}
	}

	// Преобразуем map в срез для сортировки
	sortedResults := make([]tline, 0, len(allResults))
	for _, value := range allResults {
		sortedResults = append(sortedResults, value)
	}

	// Реализация пузырьковой сортировки по атрибуту summ
	n := len(sortedResults)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			// Сортировка по убыванию summ
			if sortedResults[j].summ < sortedResults[j+1].summ {
				// Меняем элементы местами
				sortedResults[j], sortedResults[j+1] = sortedResults[j+1], sortedResults[j]
			}
		}
	}

	if format == "csv" {
		// Сохраняем отсортированный срез в CSV в stdout
		exportCSV(os.Stdout, sortedResults)
	} else if format == "json" {
		// Сохраняем отсортированный срез в JSON в stdout
		exportJSON(os.Stdout, sortedResults)
	}
}

func getHash(inStr string) uint64 {
	return fnv1a.HashString64(inStr)
}
