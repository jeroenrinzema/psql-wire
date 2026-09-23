package wire

import (
	"regexp"
	"strconv"
	"strings"
)

// QueryParameters represents a regex which could be used to identify and lookup
// parameters defined inside a given query. Parameters could be defined as
// [positional parameters] and non-positional parameters.
//
// [positional parameters]: https://www.postgresql.org/docs/15/sql-expressions.html#SQL-EXPRESSIONS-PARAMETERS-POSITIONAL
var QueryParameters = regexp.MustCompile(`\$(\d+)|\?`)

// ParseParameters returns a zero-valued slice whose length equals the number
// of parameters expected by the extended query protocol. PostgreSQL string
// literals (single-quoted, escape, dollar-quoted) and comments (line,
// nested block) are skipped so that $N tokens inside them are not mistaken
// for parameters.
func ParseParameters(query string) []uint32 {
	const tokenStarts = "-/Ee'$?"
	var length int
	n := len(query)
	for i := 0; i < n; {
		jump := strings.IndexAny(query[i:], tokenStarts)
		if jump < 0 {
			break
		}
		i += jump
		switch c := query[i]; {
		case c == '-' && i+1 < n && query[i+1] == '-':
			i = skipLineComment(query, i)
		case c == '/' && i+1 < n && query[i+1] == '*':
			i = skipBlockComment(query, i)
		case (c == 'E' || c == 'e') && i+1 < n && query[i+1] == '\'' &&
			(i == 0 || !isIdentCont(query[i-1])):
			i = skipEscapeString(query, i)
		case c == '\'':
			i = skipSingleQuote(query, i)
		case c == '$':
			if pos, end, ok := tryParameter(query, i); ok {
				if pos > length {
					length = pos
				}
				i = end
			} else if end, ok := skipDollarQuote(query, i); ok {
				i = end
			} else {
				i++
			}
		case c == '?':
			length++
			i++
		default:
			// 'e' or 'E' that didn't qualify as escape-string prefix.
			i++
		}
	}
	return make([]uint32, length)
}

// tryParameter attempts to read a positional parameter ($<digits>) starting
// at query[i], which must be '$'. On success, returns the position and the
// index just past the digits. On failure (e.g. $$ or $tag), returns ok=false.
func tryParameter(query string, i int) (pos, end int, ok bool) {
	j := i + 1
	for j < len(query) && query[j] >= '0' && query[j] <= '9' {
		j++
	}
	if j == i+1 {
		return 0, 0, false
	}
	pos, _ = strconv.Atoi(query[i+1 : j]) //nolint:errcheck
	return pos, j, true
}

// skipLineComment skips `-- ... \n` starting at query[i]. Returns the index
// just past the terminating newline (or len(query) if EOF reached first).
func skipLineComment(query string, i int) int {
	if idx := strings.IndexByte(query[i+2:], '\n'); idx >= 0 {
		return i + 2 + idx + 1
	}
	return len(query)
}

// skipBlockComment skips `/* ... */` starting at query[i], honoring
// PostgreSQL's nested-block-comment semantics. Returns the index just past
// the matching close (or len(query) if unterminated).
func skipBlockComment(query string, i int) int {
	depth := 1
	j := i + 2
	for depth > 0 {
		open := strings.Index(query[j:], "/*")
		closeIdx := strings.Index(query[j:], "*/")
		if closeIdx < 0 {
			return len(query)
		}
		if open >= 0 && open < closeIdx {
			depth++
			j += open + 2
		} else {
			depth--
			j += closeIdx + 2
		}
	}
	return j
}

// skipSingleQuote skips a `'...'` literal starting at query[i] (which must
// be `'`). Doubled `''` is treated as an embedded quote. Returns the index
// just past the closing quote (or len(query) if unterminated).
func skipSingleQuote(query string, i int) int {
	j := i + 1
	for {
		idx := strings.IndexByte(query[j:], '\'')
		if idx < 0 {
			return len(query)
		}
		j += idx
		if j+1 < len(query) && query[j+1] == '\'' {
			j += 2
			continue
		}
		return j + 1
	}
}

// skipEscapeString skips an `E'...'` / `e'...'` literal starting at
// query[i]. Backslash escapes any following byte (notably `\'` and `\\`).
// Returns the index just past the closing quote.
func skipEscapeString(query string, i int) int {
	j := i + 2
	for j < len(query) {
		idx := strings.IndexAny(query[j:], `\'`)
		if idx < 0 {
			return len(query)
		}
		j += idx
		if query[j] == '\\' {
			if j+1 >= len(query) {
				return len(query)
			}
			j += 2
			continue
		}
		return j + 1
	}
	return j
}

// skipDollarQuote attempts to skip a `$tag$ ... $tag$` literal starting at
// query[i] (which must be `$`). The tag may be empty (`$$`) and must not be
// a pure-digit run (which would denote a parameter). On success, returns
// the index just past the closing tag and ok=true. On failure (the `$` is
// not a quote opener), returns ok=false so the caller can fall through.
func skipDollarQuote(query string, i int) (int, bool) {
	j := i + 1
	tagEnd := j
	for tagEnd < len(query) && isIdentCont(query[tagEnd]) {
		tagEnd++
	}
	if tagEnd >= len(query) || query[tagEnd] != '$' || isDigitRun(query[j:tagEnd]) {
		return 0, false
	}
	closer := query[i : tagEnd+1] // "$" + tag + "$"
	idx := strings.Index(query[tagEnd+1:], closer)
	if idx < 0 {
		return len(query), true
	}
	return tagEnd + 1 + idx + len(closer), true
}

func isIdentStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isIdentCont(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9')
}

func isDigitRun(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
