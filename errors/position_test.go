package errors_test

import (
	"errors"
	"fmt"
	"testing"

	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/stretchr/testify/require"
)

func TestPosition(t *testing.T) {
	t.Parallel()

	cause := errors.New("syntax error")
	positioned := psqlerr.WithPosition(cause, 12)
	for _, tt := range []struct {
		name string
		err  error
		want int32
	}{
		{name: "nil"},
		{name: "absent", err: cause},
		{name: "direct", err: positioned, want: 12},
		{name: "wrapped", err: fmt.Errorf("query failed: %w", positioned), want: 12},
		{name: "outer position wins", err: psqlerr.WithPosition(positioned, 34), want: 34},
		{name: "zero clears position", err: psqlerr.WithPosition(positioned, 0)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, psqlerr.GetPosition(tt.err))
			require.Equal(t, tt.want, psqlerr.Flatten(tt.err).Position)
			if tt.err != nil {
				require.ErrorIs(t, tt.err, cause)
			}
		})
	}

	require.Nil(t, psqlerr.WithPosition(nil, 12))
	require.Equal(t, cause.Error(), positioned.Error())
}
