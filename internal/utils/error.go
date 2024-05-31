// Package implements internal utility functions

package utils

import (
	"log"
)

// If there is an error, logs the error then calls os.Exit(1)
//
// Parameters:
// 	- err
func CheckFatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
