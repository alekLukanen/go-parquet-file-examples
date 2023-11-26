package goparquetfileexamples

import (
	//"context"
	//"errors"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"

	"github.com/apache/arrow/go/v14/parquet"
	parquetFileUtils "github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/metadata"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
)

type MergeFile struct {
	fileWriter  *pqarrow.FileWriter
	filePath    string
	maxRows     int
	rowsWritten int
}

func NewMergeFile(filePath string, maxRows int) (MergeFile, error) {
	// the file writter will close this file
	return MergeFile{
		fileWriter:  nil,
		filePath:    filePath,
		maxRows:     maxRows,
		rowsWritten: 0,
	}, nil
}

func (obj *MergeFile) CreateWriter(schema *arrow.Schema) error {
	file, err := os.Create(obj.filePath)
	if err != nil {
		return err
	}

	parquetWriteProps := parquet.NewWriterProperties(
		parquet.WithMaxRowGroupLength(100_000), parquet.WithStatsFor("A", true),
	)
	arrowWriteProps := pqarrow.NewArrowWriterProperties()
	parquetFileWriter, err := pqarrow.NewFileWriter(schema, file, parquetWriteProps, arrowWriteProps)
	obj.fileWriter = parquetFileWriter

	return nil
}

func (obj *MergeFile) AddRecordData(record arrow.Record, startIndex, stopIndex int64) error {
	if !obj.FileCreated() {
		if err := obj.CreateWriter(record.Schema()); err != nil {
			return err
		}
	}

	// from the source record copy over the data in the
	// provided range
	recordSlice := record.NewSlice(startIndex, stopIndex)
	defer recordSlice.Release()

	err := obj.fileWriter.Write(recordSlice)
	if err != nil {
		return err
	}

	obj.rowsWritten += int(record.NumRows())

	return nil
}

func (obj *MergeFile) Full() bool {
	return obj.rowsWritten >= obj.maxRows
}

func (obj *MergeFile) FileCreated() bool {
	return obj.fileWriter != nil
}

func (obj *MergeFile) String() string {
	return fmt.Sprintf("MergeFile: filePath: %s, maxRows: %d, rowsWritten: %d", obj.filePath, obj.maxRows, obj.rowsWritten)
}

func (obj *MergeFile) Close() {
	if obj.FileCreated() {
		obj.fileWriter.Close()
	}
}

func MergeSortedFiles(fileSize, fileCount int) error {
	if fileSize < 1 || fileCount < 2 {
		return errors.New("incorrect file attributes")
	}

	ctx := context.Background()

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
	mergeFiles := make([]*MergeFile, 0)
	mergeFilePath := func() string {
		return path.Join(dataDir, fmt.Sprintf("merge-file-%d.parquet", len(mergeFiles)))
	}

	for fileIndex := 0; fileIndex < len(files); fileIndex++ {

		fmt.Printf("merging file: %d\n", fileIndex)

		fileRecordReader, err := FileRecordReader(ctx, pool, files[fileIndex])

		for fileRecordReader.Next() {

			if len(mergeFiles) == 0 || mergeFiles[len(mergeFiles)-1].Full() {

				fmt.Println("creating new merge file")

				// close the previous file
				if len(mergeFiles) > 0 {
					fmt.Println("closing merge file")
					mergeFiles[len(mergeFiles)-1].Close()
				}

				mergeFile, err := NewMergeFile(mergeFilePath(), 2*fileSize)
				if err != nil {
					return err
				}
				defer mergeFile.Close()

				mergeFiles = append(mergeFiles, &mergeFile)
			}

			currentMergeFile := mergeFiles[len(mergeFiles)-1]

			fmt.Println(currentMergeFile.String())

			record := fileRecordReader.Record()
			err = currentMergeFile.AddRecordData(record, 0, record.NumRows())
			if err != nil {
				return err
			}
			record.Release()
		}

	}

	if len(mergeFiles) > 0 {
		fmt.Println("closing merge file")
		mergeFiles[len(mergeFiles)-1].Close()
	}

	fmt.Println("merge file stats")
	for index, mergeFile := range mergeFiles {
		fmt.Printf("- merge file: %d", index)
		err = PrintColumnStats(mergeFile.filePath)
		if err != nil {
			return err
		}
	}

	return nil
}

func FileRecordReader(ctx context.Context, pool *memory.GoAllocator, filePath string) (pqarrow.RecordReader, error) {
	parquetFileReader, err := parquetFileUtils.OpenParquetFile(filePath, false)
	if err != nil {
		return nil, err
	}

	parquetReadProps := pqarrow.ArrowReadProperties{
		Parallel:  false,
		BatchSize: 1024 * 10,
	}
	arrowFileReader, err := pqarrow.NewFileReader(parquetFileReader, parquetReadProps, pool)
	if err != nil {
		return nil, err
	}

	recordReader, err := arrowFileReader.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return nil, err
	}

	return recordReader, nil
}

func PrintColumnStats(filePath string) error {
	fmt.Println("reading parquet file")

	parquetFileReader, err := parquetFileUtils.OpenParquetFile(filePath, false)
	if err != nil {
		return err
	}

	fmt.Printf("number of row groups: %d\n", parquetFileReader.NumRowGroups())
	fmt.Printf("number of rows: %d\n", parquetFileReader.NumRows())

	minValue, maxValue, _ := GetFileMaxAndMin(parquetFileReader)

	fmt.Printf("min: %d\n", minValue)
	fmt.Printf("max: %d\n", maxValue)

	return nil
}

func GetFileMaxAndMin(parquetFileReader *parquetFileUtils.Reader) (interface{}, interface{}, parquet.Type) {
	schema := parquetFileReader.MetaData().Schema
	columnType := schema.Column(0).PhysicalType()

	fileMetaData := parquetFileReader.MetaData()
	minRowGroupStats := (*fileMetaData).RowGroups[0].Columns[0].MetaData.Statistics
	maxRowGroupStats := (*fileMetaData).RowGroups[len((*fileMetaData).RowGroups)-1].Columns[0].MetaData.Statistics

	minValue := metadata.GetStatValue(columnType, minRowGroupStats.MinValue)
	maxValue := metadata.GetStatValue(columnType, maxRowGroupStats.MaxValue)

	return minValue, maxValue, columnType
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
			{Name: "A", Type: arrow.PrimitiveTypes.Int32, Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"1"})},
			{Name: "B", Type: arrow.PrimitiveTypes.Float64, Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"2"})},
		},
		nil,
	)
	parquetWriteProps := parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(100_000), parquet.WithStatsFor("A", true))
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

	column1, column2 := CreateExampleSortedData(fileIndex, fileSize)

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

func CreateExampleSortedData(fileIndex int, fileSize int) ([]int32, []float64) {
	offset := fileIndex * fileSize
	// 0 -> 0 to 9
	// 1 -> 10 to 19
	// ...
	column1 := make([]int32, fileSize)
	column2 := make([]float64, fileSize)
	for i := 0; i < fileSize; i++ {
		column1[i] = int32(i + offset)
		column2[i] = rand.Float64() * 10.0
	}

	return column1, column2
}
