package wire

import (
	"context"
	"strconv"
	"testing"

	"github.com/lib/pq/oid"
)

func TestInvalidOptions(t *testing.T) {
	tests := [][]OptionFn{
		{
			Parse(func(context.Context, string) (PreparedStatementFn, []oid.Oid, error) { return nil, nil, nil }),
			SimpleQuery(func(context.Context, string, DataWriter, []string) error { return nil }),
		},
	}

	for index, test := range tests {
		t.Run(strconv.Itoa(index), func(t *testing.T) {
			srv := &Server{}
			for _, option := range test {
				err := option(srv)
				if err != nil {
					return
				}
			}

			t.Error("unexpected pass")
		})
	}
}
