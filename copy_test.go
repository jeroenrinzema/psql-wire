package wire

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/neilotoole/slogt"
)

func TestCopyReaderText(t *testing.T) {
	table := Columns{
		{
			Table: 0,
			Name:  "id",
			Oid:   pgtype.Int4OID,
			Width: 4,
		},
		{
			Table: 0,
			Name:  "name",
			Oid:   pgtype.TextOID,
			Width: 256,
		},
		{
			Table: 0,
			Name:  "member",
			Oid:   pgtype.BoolOID,
			Width: 1,
		},
		{
			Table: 0,
			Name:  "age",
			Oid:   pgtype.Int4OID,
			Width: 1,
		},
	}

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		log.Println("incoming SQL query:", query)

		handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			log.Println("copying data")

			copyText, err := writer.CopyIn(TextFormat)
			if err != nil {
				return err
			}

			var length int
			csvReaderBuffer := &bytes.Buffer{}
			csvReader := csv.NewReader(csvReaderBuffer)
			csvReader.Comma = ','
			csvReader.TrimLeadingSpace = false
			csvReader.LazyQuotes = true
			reader, err := NewTextColumnReader(ctx, copyText, csvReader, csvReaderBuffer, "")
			if err != nil {
				return err
			}

			for {
				columns, err := reader.Read(ctx)
				if err == io.EOF {
					break
				}

				if err != nil {
					return err
				}

				t.Logf("received columns: %+v", columns)
				length++
			}

			return writer.Complete(fmt.Sprintf("COPY %d", length))
		}

		return Prepared(NewStatement(handle, WithColumns(table))), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)

	ctx := context.Background()
	connStr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)

	t.Run("CopyInStmtFromStdinText", func(t *testing.T) {
		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx) //nolint:errcheck

		file, err := os.Open("jedis.csv")
		if err != nil {
			log.Fatalf("failed to open CSV file: %s", err.Error())
		}

		query := `COPY "public"."jedis" FROM STDIN WITH DELIMITER ',' CSV`

		_, err = conn.PgConn().CopyFrom(
			ctx,
			file,
			query,
		)
		if err != nil {
			t.Fatalf("copy stmt failed: %s \n", err.Error())
		}
	})
}

func TestCopyReaderTextNullAndEscape(t *testing.T) {
	table := Columns{
		{
			Table: 0,
			Name:  "id",
			Oid:   pgtype.Int4OID,
			Width: 4,
		},
		{
			Table: 0,
			Name:  "name",
			Oid:   pgtype.TextOID,
			Width: 256,
		},
		{
			Table: 0,
			Name:  "member",
			Oid:   pgtype.BoolOID,
			Width: 1,
		},
		{
			Table: 0,
			Name:  "age",
			Oid:   pgtype.Int4OID,
			Width: 1,
		},
		{
			Table: 0,
			Name:  "description",
			Oid:   pgtype.TextOID,
		},
	}

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		log.Println("incoming SQL query:", query)

		handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			log.Println("copying data")

			copyText, err := writer.CopyIn(TextFormat)
			if err != nil {
				return err
			}

			var length int
			csvReaderBuffer := &bytes.Buffer{}
			csvReader := csv.NewReader(csvReaderBuffer)
			csvReader.Comma = ','
			csvReader.TrimLeadingSpace = false
			csvReader.LazyQuotes = true
			reader, err := NewTextColumnReader(ctx, copyText, csvReader, csvReaderBuffer, "attNULL")
			if err != nil {
				return err
			}

			for {
				columns, err := reader.Read(ctx)
				if err == io.EOF {
					break
				}

				if err != nil {
					return err
				}

				t.Logf("received columns: %+v", columns)
				length++
			}

			return writer.Complete(fmt.Sprintf("COPY %d", length))
		}

		return Prepared(NewStatement(handle, WithColumns(table))), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)

	ctx := context.Background()
	connStr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)

	t.Run("CopyInStmtFromStdinTextNullAndEscape", func(t *testing.T) {
		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx) //nolint:errcheck

		file, err := os.Open("jedis_null_escape.csv")
		if err != nil {
			log.Fatalf("failed to open CSV file: %s", err.Error())
		}
		query := `COPY "public"."jedis" FROM STDIN WITH DELIMITER ',' CSV NULL 'attNULL' ESCAPE '\'`

		_, err = conn.PgConn().CopyFrom(
			ctx,
			file,
			query,
		)
		if err != nil {
			t.Fatalf("copy stmt failed: %s \n", err.Error())
		}
	})
}
