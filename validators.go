package gharchive

import (
	"time"

	jsoniter "github.com/json-iterator/go"
)

var whitespace = [256]bool{
	' ':  true,
	'\r': true,
	'\n': true,
	'\t': true,
}

// ValidateNotEmpty validate that line contains at least one non-whitespace character
func ValidateNotEmpty() Validator {
	return func(line []byte) bool {
		for _, b := range line {
			if !whitespace[b] {
				return true
			}
		}
		return false
	}
}

// ValidateIsJSONObject returns true if the first non-whitespace byte is '{'
func ValidateIsJSONObject() Validator {
	return func(line []byte) bool {
		for _, b := range line {
			if whitespace[b] {
				continue
			}
			return b == '{'
		}
		return false
	}
}

// JSONValueValidator validates a json value
type JSONValueValidator func(val interface{}) bool

// JSONFieldValidator validates the value of a json field
type JSONFieldValidator struct {
	Field     string
	Validator JSONValueValidator
}

// ValidateJSONFields uses the given validators to validate json field
func ValidateJSONFields(validators []JSONFieldValidator) Validator {
	return func(line []byte) bool {
		iter := jsoniter.ConfigFastest.BorrowIterator(line)
		defer jsoniter.ConfigFastest.ReturnIterator(iter)
		done := make([]bool, len(validators))
		allDone := func() bool {
			for _, b := range done {
				if !b {
					return false
				}
			}
			return true
		}
		valid := true
		iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
			var val interface{}
			for i := range validators {
				if allDone() {
					break
				}
				valField := validators[i].Field
				if valField != field {
					iter.Skip()
					continue
				}
				if done[i] {
					iter.Skip()
					continue
				}
				if val == nil {
					val = iter.ReadAny().GetInterface()
				}
				validateFunc := validators[i].Validator
				valid = validateFunc(val)
				done[i] = true
				if val == nil {
					iter.Skip()
				}
				if !valid {
					return false
				}
			}
			ad := allDone()
			return !ad
		})
		if !allDone() {
			return false
		}
		return valid
	}
}

// StringValueValidator validates a string value
func StringValueValidator(validate func(val string) bool) JSONValueValidator {
	return func(val interface{}) bool {
		strVal, ok := val.(string)
		if !ok {
			return false
		}
		return validate(strVal)
	}
}

// TimeValueValidator validates a time value
func TimeValueValidator(validate func(val time.Time) bool) JSONValueValidator {
	return StringValueValidator(func(val string) bool {
		createdAt, err := time.Parse(time.RFC3339, val)
		if err != nil {
			return false
		}
		return validate(createdAt)
	})
}
