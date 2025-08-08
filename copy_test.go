package wire

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/lib/pq/oid"
	"github.com/neilotoole/slogt"
	"io"
	"log"
	"os"
	"testing"
)

func TestCopyReaderText(t *testing.T) {
	table := Columns{
		{
			Table: 0,
			Name:  "id",
			Oid:   oid.T_int4,
			Width: 4,
		},
		{
			Table: 0,
			Name:  "name",
			Oid:   oid.T_text,
			Width: 256,
		},
		{
			Table: 0,
			Name:  "member",
			Oid:   oid.T_bool,
			Width: 1,
		},
		{
			Table: 0,
			Name:  "age",
			Oid:   oid.T_int4,
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
			reader, err := NewTextColumnReader(ctx, copyText, TextCopyReaderOptions{
				separator: ',',
				nullValue: "",
			})
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
		defer conn.Close(ctx)

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
			Oid:   oid.T_int4,
			Width: 4,
		},
		{
			Table: 0,
			Name:  "name",
			Oid:   oid.T_text,
			Width: 256,
		},
		{
			Table: 0,
			Name:  "member",
			Oid:   oid.T_bool,
			Width: 1,
		},
		{
			Table: 0,
			Name:  "age",
			Oid:   oid.T_int4,
			Width: 1,
		},
		{
			Table: 0,
			Name:  "description",
			Oid:   oid.T_text,
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
			reader, err := NewTextColumnReader(ctx, copyText, TextCopyReaderOptions{
				separator: ',',
				nullValue: "attNULL",
			})
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
		defer conn.Close(ctx)

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
