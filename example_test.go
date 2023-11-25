package goparquetfileexamples

import (
	"testing"
)

func TestWriteAndReadSimpleFile(t *testing.T) {
	err := WriteAndReadSimpleFile()
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteAndReadMultipleRowFile(t *testing.T) {
	err := WriteAndReadMultipleRowFile()
	if err != nil {
		t.Fatal(err)
	}
}

func TestMergeSortedFiles(t *testing.T) {
	err := MergeSortedFiles(10, 3)
	if err != nil {
		t.Fatal(err)
	}
}
