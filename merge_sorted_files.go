package goparquetfileexamples

import (
	//"context"
	//"errors"
	"fmt"
	"math/rand"
	"os"
	"path"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"

	"github.com/apache/arrow/go/v14/parquet"
	parquetFileUtils "github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
)

func MergeSortedFiles(fileSize, fileCount int) error {

	dataDir, err := os.MkdirTemp("", "MergeSortedFiles")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dataDir)

	files, err := CreateSortedFiles(fileSize, fileCount, dataDir)
	if err != nil {
		return err
	}

	fmt.Println(files)

	pool := memory.NewGoAllocator()
	for _, filePath := range files {
		PrintColumnStats(filePath, pool)
	}

	return nil
}

func MergeFilePair(filePath1, filePath2 string) (string, error) {
	return "", nil
}

func PrintColumnStats(filePath string, pool memory.Allocator) error {
	fmt.Println("reading parquet file")

	// ctx := context.Background()

	parquetFileReader, err := parquetFileUtils.OpenParquetFile(filePath, false)
	if err != nil {
		return err
	}

	fmt.Printf("number of row groups: %d\n", parquetFileReader.NumRowGroups())
	fmt.Printf("number of rows: %d", parquetFileReader.NumRows())

	fileMetaData := parquetFileReader.MetaData()
	rowGroup := (*fileMetaData).RowGroups[0]
	stats := rowGroup.Columns[0].MetaData.Statistics

	fmt.Printf("min: %d\n", stats.MinValue)
	fmt.Printf("max: %d\n", stats.MaxValue)

	/*

		parquetRowGroupReader := parquetFileReader.RowGroup(0)
		rowGroupMetaData := parquetRowGroupReader.MetaData()

		columnChunkMetaData, err := rowGroupMetaData.ColumnChunk(0)
		if err != nil {
			return err
		}

		columnChunkMetaData.


		stats, err := columnChunkMetaData.Statistics()
		if err != nil {
			return err
		}

		if !stats.HasMinMax() {
			return errors.New("column chunck missing stats")
		}

		encodedStats, err := stats.Encode()
		if err != nil {
			return err
		}

		encodedStats.Max()

		parquetReadProps := pqarrow.ArrowReadProperties{
			Parallel:  false,
			BatchSize: 1024 * 10,
		}
		parquetReadProps.SetReadDict(0, true)
		parquetReadProps.SetReadDict(1, true)
		arrowFileReader, err := pqarrow.NewFileReader(parquetFileReader, parquetReadProps, pool)
		if err != nil {
			return err
		}

		// Get the schema
		schema, err := arrowFileReader.Schema()
		if err != nil {
			return err
		}

		// get the column reader
		columnReader, err := arrowFileReader.GetColumn(ctx, 0)
		if err != nil {
			return err
		}


		columnReader.Field()

		// Get the column statistics
		stats, err := columnReader.a()
		if err != nil {
			return err
		}

		// Access statistics information
		minValue := stats.Min()
		maxValue := stats.Max()
		nullCount := stats.NullCount()

		// Print or use the statistics information as needed
		log.Printf("Min Value: %v, Max Value: %v, Null Count: %v", minValue, maxValue, nullCount)

	*/

	return nil
}

func CreateSortedFiles(fileSize int, fileCount int, dataDir string) ([]string, error) {
	fmt.Println("CreateSortedFiles()")

	files := make([]string, fileCount)

	for i := 0; i < fileCount; i++ {
		filePath, err := CreateSortedFile(i, fileSize, dataDir)
		if err != nil {
			return nil, err
		}
		files[i] = filePath
	}

	return files, nil
}

func CreateSortedFile(fileIndex int, fileSize int, dataDir string) (string, error) {
	/*
		Creates a sorted file where column A is the sorted column.
	*/

	fmt.Println("CreateSortedFile()")

	filePath := path.Join(dataDir, fmt.Sprintf("sorted-file-%d.parquet", fileIndex))

	file, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// define the parquet schema and open the file for writing
	fmt.Println("--- writing parquet file")

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "A", Type: arrow.PrimitiveTypes.Int32},
			{Name: "B", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)
	parquetWriteProps := parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(10), parquet.WithStatsFor("A", true))
	arrowWriteProps := pqarrow.NewArrowWriterProperties()
	parquetFileWriter, err := pqarrow.NewFileWriter(schema, file, parquetWriteProps, arrowWriteProps)
	if err != nil {
		return "", err
	}
	defer parquetFileWriter.Close()

	pool := memory.NewGoAllocator()

	// create a record and write it to the file
	b1 := array.NewRecordBuilder(pool, schema)
	defer b1.Release()

	column1, column2 := CreateExampleSortedData(fileIndex)

	b1.Field(0).(*array.Int32Builder).AppendValues(column1, nil)
	b1.Field(1).(*array.Float64Builder).AppendValues(column2, nil)

	rec1 := b1.NewRecord()
	defer rec1.Release()

	// write the record to the first row group
	err = parquetFileWriter.Write(rec1)
	if err != nil {
		return "", err
	}

	// close the writer
	parquetFileWriter.Close()

	return filePath, nil
}

func CreateExampleSortedData(fileIndex int) ([]int32, []float64) {
	offset := fileIndex * 10
	// 0 -> 0 to 9
	// 1 -> 10 to 19
	// ...
	column1 := make([]int32, 10)
	column2 := make([]float64, 10)
	for i := 0; i < 10; i++ {
		column1[i] = int32(i + offset)
		column2[i] = rand.Float64() * 10.0
	}

	return column1, column2
}
