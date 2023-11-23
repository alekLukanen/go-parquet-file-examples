package goparquetfileexamples

import (
	"fmt"
	"os"
	"path"
	"context"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"

	"github.com/apache/arrow/go/v14/parquet"
	parquetFileUtils "github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
)


func WriteAndReadSimpleFile() error {
	fmt.Println("writeAndReadSimpleFile()")

	dataDir, err := os.MkdirTemp("", "WriteAndReadSimpleFile")
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
	parquetWriteProps := parquet.NewWriterProperties()
	arrowWriteProps := pqarrow.NewArrowWriterProperties()
	parquetFileWriter, err := pqarrow.NewFileWriter(schema, file, parquetWriteProps, arrowWriteProps)
	if err != nil {
		return err
	}
	defer parquetFileWriter.Close()

	// create a record and write it to the file
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4, 5, 6}, nil)
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{7, 8, 9, 10}, []bool{true, true, false, true})
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)

	rec := b.NewRecord()
	defer rec.Release()

	for i, col := range rec.Columns() {
		fmt.Printf("column[%d] %q: %v\n", i, rec.ColumnName(i), col)
	}

	err = parquetFileWriter.Write(rec)
	if err != nil {
		return err
	}


	ctx := context.Background()

	// get the file size
	fi, err = file.Stat()
	if err != nil {
		return err
	}
	fmt.Printf("The file is %d bytes long\n", fi.Size())

	// close the writer
	parquetFileWriter.Close()
	file.Close()

	// open the file for reading
	fmt.Println("reading parquet file")

	parquetFileReader, err := parquetFileUtils.OpenParquetFile(filePath, false)
	parquetReadProps := pqarrow.ArrowReadProperties{
		Parallel: false,
		BatchSize: 1024 * 10,
	}
	parquetReadProps.SetReadDict(0, true)
	parquetReadProps.SetReadDict(1, true)
	arrowFileReader, err := pqarrow.NewFileReader(parquetFileReader, parquetReadProps, pool)
	if err != nil {
		return err
	}

	recordReader, err := arrowFileReader.GetRecordReader(ctx, nil, nil)
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