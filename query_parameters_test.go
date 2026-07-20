package wire

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseParameters(t *testing.T) {
	type test struct {
		query      string
		parameters []uint32
	}

	tests := map[string]test{
		"positional": {
			query:      "SELECT * FROM users WHERE id = $1 AND age > $2",
			parameters: []uint32{0, 0},
		},
		"unpositional": {
			query:      "SELECT * FROM users WHERE id = ? AND age > ?",
			parameters: []uint32{0, 0},
		},
		"single high-numbered positional": {
			query:      "SELECT $349",
			parameters: make([]uint32, 349),
		},
		"sparse positional": {
			query:      "SELECT $1, $349",
			parameters: make([]uint32, 349),
		},
		"mixed anonymous and positional": {
			query:      "SELECT ?, ?, $5",
			parameters: make([]uint32, 5),
		},
		"dollar-N inside single-quoted string": {
			query:      "SELECT '$349'",
			parameters: []uint32{},
		},
		"dollar-N inside dollar-quoted block": {
			query:      "SELECT $$ x $349 y $$",
			parameters: []uint32{},
		},
		"dollar-N inside line comment": {
			query:      "SELECT 1 -- $349",
			parameters: []uint32{},
		},
		"dollar-N inside block comment": {
			query:      "SELECT 1 /* $349 */",
			parameters: []uint32{},
		},
		"dollar-N inside nested block comment": {
			query:      "SELECT 1 /* $1 /* $2 */ */",
			parameters: []uint32{},
		},
		"dollar-N inside escape string": {
			query:      "SELECT E'$349'",
			parameters: []uint32{},
		},
		"dollar-N inside tagged dollar-quoted block": {
			query:      "SELECT $foo$ x $349 y $foo$",
			parameters: []uint32{},
		},
		"doubled-quote escape inside string": {
			query:      "SELECT 'it''s $5'",
			parameters: []uint32{},
		},
		"identifier ending in e then string literal": {
			// 'e' must not be misread as the escape-string prefix when it
			// follows an identifier character (e.g. column name 'type').
			query:      "SELECT type, e'$2'",
			parameters: []uint32{},
		},
		"mixed string-literal and real parameter": {
			query:      "SELECT '$1' AS a, $2",
			parameters: make([]uint32, 2),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			parameters := ParseParameters(test.query)
			assert.Equal(t, test.parameters, parameters)
		})
	}
}
