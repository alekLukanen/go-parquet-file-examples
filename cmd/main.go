package main

import (
	"fmt"

	goparquetfileexamples "github.com/alekLukanen/go-parquet-file-examples"
)


func main() {
	if err := goparquetfileexamples.WriteAndReadSimpleFile(); err != nil {
		fmt.Printf("error: %o\n", err)
		return
	}

	fmt.Println("DONE!")
}
