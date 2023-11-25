package goparquetfileexamples

import (
	"context"
	"fmt"
	"os"
	"path"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"

	"github.com/apache/arrow/go/v14/parquet"
	parquetFileUtils "github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
)

func WriteAndReadMultipleRowFile() error {
	fmt.Println("WriteAndReadMultipleRowFile()")

	ctx := context.Background()

	dataDir, err := os.MkdirTemp("", "WriteAndReadMultipleRowFile")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dataDir)

	filePath := path.Join(dataDir, "example.parquet")
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// get the file size
	fi, err := file.Stat()
	if err != nil {
		return err
	}
	fmt.Printf("The file is %d bytes long\n", fi.Size())

	// define the parquet schema and open the file for writing
	fmt.Println("writing parquet file")

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "int32Field", Type: arrow.PrimitiveTypes.Int32},
			{Name: "float64Field", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)
	parquetWriteProps := parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(2))
	arrowWriteProps := pqarrow.NewArrowWriterProperties()
	parquetFileWriter, err := pqarrow.NewFileWriter(schema, file, parquetWriteProps, arrowWriteProps)
	if err != nil {
		return err
	}
	defer parquetFileWriter.Close()

	pool := memory.NewGoAllocator()

	// create a record and write it to the file
	b1 := array.NewRecordBuilder(pool, schema)
	defer b1.Release()

	b1.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4, 5, 6}, nil)
	b1.Field(0).(*array.Int32Builder).AppendValues([]int32{7, 8, 9, 10}, []bool{true, true, false, true})
	b1.Field(1).(*array.Float64Builder).AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

	rec1 := b1.NewRecord()
	defer rec1.Release()

	for i, col := range rec1.Columns() {
		fmt.Printf("column[%d] %q: %v\n", i, rec1.ColumnName(i), col)
	}

	// write the record to the first row group
	err = parquetFileWriter.Write(rec1)
	if err != nil {
		return err
	}

	// create a second record and write it to the second row group
	b2 := array.NewRecordBuilder(pool, schema)
	defer b2.Release()

	b2.Field(0).(*array.Int32Builder).AppendValues([]int32{7, 8, 9, 10, 11, 2, 3, 5}, nil)
	b2.Field(1).(*array.Float64Builder).AppendValues([]float64{20, 21, 22, 23, 24, 25, 26, 27.77}, nil)

	rec2 := b2.NewRecord()
	defer rec2.Release()

	for i, col := range rec2.Columns() {
		fmt.Printf("column[%d] %q: %v\n", i, rec2.ColumnName(i), col)
	}

	// write the record to the second row group
	err = parquetFileWriter.Write(rec2)
	if err != nil {
		return err
	}

	// get the file size before closing the writer
	fi, err = file.Stat()
	if err != nil {
		return err
	}
	fmt.Printf("The file is %d bytes long before close\n", fi.Size())

	// close the writer
	parquetFileWriter.Close()

	// get the file size after closing the writer
	file, err = os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	fi, err = file.Stat()
	if err != nil {
		return err
	}
	fmt.Printf("The file is %d bytes long after close\n", fi.Size())

	// open the file for reading
	fmt.Println("reading parquet file")

	parquetFileReader, err := parquetFileUtils.OpenParquetFile(filePath, false)
	if err != nil {
		return err
	}

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

	fmt.Printf("number of row groups: %d\n", parquetFileReader.NumRowGroups())
	fmt.Printf("number of rows: %d", parquetFileReader.NumRows())

	recordReader, err := arrowFileReader.GetRecordReader(ctx, nil, []int{0, 5})
	if err != nil {
		return err
	}

	for recordReader.Next() {
		fmt.Println("- next record")
		record := recordReader.Record()
		for i, col := range record.Columns() {
			fmt.Printf("column[%d] %q: %v\n", i, record.ColumnName(i), col)
		}
	}

	return nil
}
