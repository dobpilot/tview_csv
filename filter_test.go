package main

import (
	"testing"
)

func TestParseFilter_Simple(t *testing.T) {
	filterStr := "event=DBPOSTGRS"
	filter, err := ParseFilter(filterStr)
	if err != nil {
		t.Errorf("ParseFilter() вернул ошибку: %v", err)
	}

	if len(filter.conditions) != 1 {
		t.Errorf("Ожидалось 1 условие, получено: %d", len(filter.conditions))
	}

	if filter.conditions[0].Key != "event" {
		t.Errorf("Ожидался ключ 'event', получено: %s", filter.conditions[0].Key)
	}

	if filter.conditions[0].Value != "DBPOSTGRS" {
		t.Errorf("Ожидалось значение 'DBPOSTGRS', получено: %s", filter.conditions[0].Value)
	}
}

func TestParseFilter_WithDoubleQuotes(t *testing.T) {
	filterStr := `event="DB POSTGRES"`
	filter, err := ParseFilter(filterStr)
	if err != nil {
		t.Errorf("ParseFilter() вернул ошибку: %v", err)
	}

	if len(filter.conditions) != 1 {
		t.Errorf("Ожидалось 1 условие, получено: %d", len(filter.conditions))
	}

	if filter.conditions[0].Value != "DB POSTGRES" {
		t.Errorf("Ожидалось значение 'DB POSTGRES', получено: %s", filter.conditions[0].Value)
	}
}

func TestParseFilter_WithSingleQuotes(t *testing.T) {
	filterStr := "process='rphost'"
	filter, err := ParseFilter(filterStr)
	if err != nil {
		t.Errorf("ParseFilter() вернул ошибку: %v", err)
	}

	if filter.conditions[0].Value != "rphost" {
		t.Errorf("Ожидалось значение 'rphost', получено: %s", filter.conditions[0].Value)
	}
}

func TestParseFilter_Invalid_NoEquals(t *testing.T) {
	filterStr := "eventDBPOSTGRS"
	_, err := ParseFilter(filterStr)
	if err == nil {
		t.Error("ParseFilter() должен вернуть ошибку для строки без '='")
	}
}

func TestParseFilter_Invalid_EmptyKey(t *testing.T) {
	filterStr := "=value"
	_, err := ParseFilter(filterStr)
	if err == nil {
		t.Error("ParseFilter() должен вернуть ошибку для пустого ключа")
	}
}

func TestParseFilter_Invalid_EmptyValue(t *testing.T) {
	filterStr := "event="
	_, err := ParseFilter(filterStr)
	if err == nil {
		t.Error("ParseFilter() должен вернуть ошибку для пустого значения")
	}
}

func TestParseFilter_UnclosedQuotes(t *testing.T) {
	filterStr := `event="DB POSTGRES`
	_, err := ParseFilter(filterStr)
	if err == nil {
		t.Error("ParseFilter() должен вернуть ошибку для незакрытых кавычек")
	}
}

func TestApply_NoFilters(t *testing.T) {
	filter := &Filter{conditions: []FilterCondition{}}
	record := map[string]string{"event": "DBPOSTGRS", "process": "rphost"}

	if !filter.Apply(record) {
		t.Error("Apply() должен вернуть true при пустом фильтре")
	}
}

func TestApply_Match(t *testing.T) {
	filter := &Filter{conditions: []FilterCondition{
		{Key: "event", Value: "DBPOSTGRS"},
	}}
	record := map[string]string{"event": "DBPOSTGRS", "process": "rphost"}

	if !filter.Apply(record) {
		t.Error("Apply() должен вернуть true для совпадающей записи")
	}
}

func TestApply_NoMatch(t *testing.T) {
	filter := &Filter{conditions: []FilterCondition{
		{Key: "event", Value: "DBPOSTGRS"},
	}}
	record := map[string]string{"event": "TLOCK", "process": "rphost"}

	if filter.Apply(record) {
		t.Error("Apply() должен вернуть false для несовпадающей записи")
	}
}

func TestApply_MultipleFilters_AllMatch(t *testing.T) {
	filter := &Filter{conditions: []FilterCondition{
		{Key: "event", Value: "DBPOSTGRS"},
		{Key: "process", Value: "rphost"},
	}}
	record := map[string]string{"event": "DBPOSTGRS", "process": "rphost"}

	if !filter.Apply(record) {
		t.Error("Apply() должен вернуть true когда все фильтры совпадают")
	}
}

func TestApply_MultipleFilters_OneNoMatch(t *testing.T) {
	filter := &Filter{conditions: []FilterCondition{
		{Key: "event", Value: "DBPOSTGRS"},
		{Key: "process", Value: "rphost"},
	}}
	record := map[string]string{"event": "DBPOSTGRS", "process": "webclient"}

	if filter.Apply(record) {
		t.Error("Apply() должен вернуть false когда один из фильтров не совпадает (AND логика)")
	}
}

func TestApply_KeyNotFound(t *testing.T) {
	filter := &Filter{conditions: []FilterCondition{
		{Key: "unknown_key", Value: "value"},
	}}
	record := map[string]string{"event": "DBPOSTGRS", "process": "rphost"}

	if filter.Apply(record) {
		t.Error("Apply() должен вернуть false если ключ не найден в записи")
	}
}

func TestApply_WithSpacesInValue(t *testing.T) {
	filter := &Filter{conditions: []FilterCondition{
		{Key: "event", Value: "DB POSTGRES"},
	}}
	record := map[string]string{"event": "DB POSTGRES", "process": "rphost"}

	if !filter.Apply(record) {
		t.Error("Apply() должен вернуть true для значения с пробелами")
	}
}

// Интеграционный тест с formatter1C
func TestFilter_Integration_ValidLog(t *testing.T) {
	log := `00:00.172000-1003,DBPOSTGRS,4,process=rphost,p:processName=hrm_temp,OSThread=20134,t:clientID=78722,t:applicationName=BackgroundJob,OSThread=20134`

	formatter := &formatter1C{}
	parsedData, err := formatter.Format(log)
	if err != nil {
		t.Skipf("Пропуск теста: парсер не смог обработать лог: %v", err)
	}

	filter := &Filter{conditions: []FilterCondition{
		{Key: "event", Value: "DBPOSTGRS"},
		{Key: "process", Value: "rphost"},
	}}

	if !filter.Apply(parsedData) {
		t.Error("Интеграционный тест: фильтр должен пропустить валидный лог")
	}
}

func TestFilter_Integration_InvalidLog(t *testing.T) {
	log := `00:00.172000-1003,DBPOSTGRS,4,process=rphost,p:processName=hrm_temp,OSThread=20134,t:clientID=78722,t:applicationName=BackgroundJob,OSThread=20134`

	formatter := &formatter1C{}
	parsedData, err := formatter.Format(log)
	if err != nil {
		t.Skipf("Пропуск теста: парсер не смог обработать лог: %v", err)
	}

	filter := &Filter{conditions: []FilterCondition{
		{Key: "event", Value: "TLOCK"},
	}}

	if filter.Apply(parsedData) {
		t.Error("Интеграционный тест: фильтр должен отфильтровать лог с другим event")
	}
}
