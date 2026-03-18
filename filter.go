// filter.go
package main

import (
	"errors"
	"strings"
)

// FilterCondition представляет одно условие фильтрации (key=value)
type FilterCondition struct {
	Key   string
	Value string
}

// Filter представляет набор условий фильтрации
// Все условия должны выполняться (логика AND)
type Filter struct {
	conditions []FilterCondition
}

// ParseFilter парсит строку фильтра в структуру Filter
// Поддерживаемые форматы:
//   - key=value
//   - key="value with spaces"
//   - key='value with spaces'
//
// Несколько фильтров передаются через отдельные вызовы и объединяются по AND
func ParseFilter(filterStr string) (*Filter, error) {
	filterStr = strings.TrimSpace(filterStr)

	if filterStr == "" {
		return &Filter{conditions: []FilterCondition{}}, nil
	}

	// Ищем первый знак '=' для разделения ключа и значения
	eqIndex := strings.Index(filterStr, "=")
	if eqIndex == -1 {
		return nil, errors.New("некорректный формат фильтра: отсутствует '='")
	}

	key := strings.TrimSpace(filterStr[:eqIndex])
	valuePart := filterStr[eqIndex+1:]

	if key == "" {
		return nil, errors.New("некорректный формат фильтра: пустой ключ")
	}

	var value string

	// Проверяем, заключено ли значение в кавычки
	if len(valuePart) >= 2 {
		quoteChar := valuePart[0]
		if (quoteChar == '"' || quoteChar == '\'') && valuePart[len(valuePart)-1] == quoteChar {
			// Закрывающая кавычка на том же месте
			value = valuePart[1 : len(valuePart)-1]
		} else if quoteChar == '"' || quoteChar == '\'' {
			// Открывающая кавычка есть, но закрывающей нет в конце
			// Ищем закрывающую кавычку
			closingIndex := strings.LastIndex(valuePart[1:], string(quoteChar))
			if closingIndex == -1 {
				return nil, errors.New("некорректный формат фильтра: незакрытые кавычки")
			}
			value = valuePart[1 : closingIndex+1]
		} else {
			value = strings.TrimSpace(valuePart)
		}
	} else {
		value = strings.TrimSpace(valuePart)
	}

	if value == "" {
		return nil, errors.New("некорректный формат фильтра: пустое значение")
	}

	return &Filter{
		conditions: []FilterCondition{
			{Key: key, Value: value},
		},
	}, nil
}

// AddCondition добавляет ещё одно условие к фильтру (логика AND)
func (f *Filter) AddCondition(key, value string) {
	f.conditions = append(f.conditions, FilterCondition{Key: key, Value: value})
}

// Apply проверяет, соответствует ли запись всем условиям фильтра
// Возвращает true если все условия выполняются (или фильтр пуст)
func (f *Filter) Apply(record map[string]string) bool {
	if len(f.conditions) == 0 {
		return true // Пустой фильтр пропускает всё
	}

	for _, cond := range f.conditions {
		recordValue, exists := record[cond.Key]
		if !exists {
			return false // Ключ не найден в записи
		}
		if recordValue != cond.Value {
			return false // Значение не совпадает
		}
	}

	return true // Все условия выполнены
}

// IsEmpty возвращает true если фильтр не содержит условий
func (f *Filter) IsEmpty() bool {
	return len(f.conditions) == 0
}
