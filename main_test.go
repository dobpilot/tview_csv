package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestExportJSON(t *testing.T) {
	results := map[string]tline{
		"key1": {count: 1, summ: 10, max: 10, avg: 10, keys: []string{"DBMSSQL"}},
		"key2": {count: 2, summ: 20, max: 15, avg: 10, keys: []string{"DBMSSQL", "SELECT 1"}},
		"key3": {count: 2, summ: 20, max: 15, avg: 10, keys: []string{"DBMSSQL"}},
	}

	group = "event"
	groupField = strings.Split(group, ",")

	var buf bytes.Buffer

	exportJSON(&buf, results)

	var actual []map[string]interface{}
	json.Unmarshal(buf.Bytes(), &actual)

	if len(actual) != 3 {
		t.Errorf("Ожидалось 3 элемента, получено: %d", len(actual))
	}

	for i, item := range actual {
		fmt.Println("key" + strconv.Itoa(i+1))
		etalon := results["key"+strconv.Itoa(i+1)]

		if item["Count"] != float64(etalon.count) {
			t.Errorf("Ожидалось: %d, получено: %v", etalon.count, item["Count"])
		}
		if item["Summ"] != float64(etalon.summ) {
			t.Errorf("Ожидалось: %d, получено: %v", etalon.summ, item["Summ"])
		}
		if item["Max"] != float64(etalon.max) {
			t.Errorf("Ожидалось: %d, получено: %v", etalon.max, item["Max"])
		}

		if item["Avg"] != float64(etalon.avg) {
			t.Errorf("Ожидалось: %d, получено: %v", etalon.avg, item["Avg"])
		}

		if item["event"] != etalon.keys[0] {
			t.Errorf("Ожидалось: %s, получено: %s", etalon.keys[0], item["event"])
		}

	}
}

func TestExportCSV(t *testing.T) {
	results := map[string]tline{
		"key1": {count: 1, summ: 10, max: 10, keys: []string{"value1"}},
		"key2": {count: 2, summ: 20, max: 15, keys: []string{"value2"}},
	}

	group = "event"
	groupField = strings.Split(group, ",")

	var buf bytes.Buffer
	exportCSV(&buf, results)

	expected := "Count,Summ,Max\n1,10,10,value1\n2,20,15,value2\n"
	if buf.String() != expected {
		t.Errorf("Ожидалось: %s, получено: %s", expected, buf.String())
	}
}
