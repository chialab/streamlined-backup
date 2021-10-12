package utils

import "fmt"

func ToError(val interface{}) error {
	if err, ok := val.(error); ok {
		return err
	}

	return fmt.Errorf("%+v", val)
}
