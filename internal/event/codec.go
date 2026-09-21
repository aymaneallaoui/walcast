package event

import (
	"bytes"
	"unicode/utf8"
)

const (
	oidBool    = 16
	oidInt8    = 20
	oidInt2    = 21
	oidInt4    = 23
	oidOID     = 26
	oidJSON    = 114
	oidFloat4  = 700
	oidFloat8  = 701
	oidNumeric = 1700
	oidJSONB   = 3802
)

const (
	hexDigits       = "0123456789abcdef"
	replacementChar = "\uFFFD"
)

var (
	nan    = []byte("NaN")
	posInf = []byte("Infinity")
	negInf = []byte("-Infinity")
)

// appendValue maps a pgoutput text value to JSON by type OID. Numeric stays a string
// to keep precision; types without a native JSON form keep their Postgres text form.
func appendValue(dst []byte, oid uint32, v []byte) []byte {
	switch oid {
	case oidBool:
		if len(v) == 1 && v[0] == 't' {
			return append(dst, "true"...)
		}
		return append(dst, "false"...)
	case oidInt2, oidInt4, oidInt8, oidOID:
		return append(dst, v...)
	case oidFloat4, oidFloat8:
		if bytes.Equal(v, nan) || bytes.Equal(v, posInf) || bytes.Equal(v, negInf) {
			return appendString(dst, v)
		}
		return append(dst, v...)
	case oidJSONB:
		return append(dst, v...)
	case oidJSON:
		return appendSingleLine(dst, v)
	default:
		return appendString(dst, v)
	}
}

// appendSingleLine drops raw line breaks, which json (unlike jsonb) preserves. They are
// always insignificant whitespace in valid JSON and would break one-event-per-line framing.
func appendSingleLine(dst, v []byte) []byte {
	for _, c := range v {
		if c != '\n' && c != '\r' {
			dst = append(dst, c)
		}
	}
	return dst
}

// appendString also escapes U+0085, U+2028 and U+2029: they are legal in JSON, but many
// line-splitting consumers treat them as line breaks and would cut an event in half.
func appendString(dst, s []byte) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c < 0x20 || c == '"' || c == '\\' {
				dst = append(dst, s[start:i]...)
				dst = appendEscapedASCII(dst, c)
				start = i + 1
			}
			i++
			continue
		}

		r, size := utf8.DecodeRune(s[i:])
		switch {
		case r == utf8.RuneError && size == 1:
			dst = append(dst, s[start:i]...)
			dst = append(dst, replacementChar...)
			start = i + size
		case r == '\u0085' || r == '\u2028' || r == '\u2029':
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', hexDigits[r>>12&0xf], hexDigits[r>>8&0xf], hexDigits[r>>4&0xf], hexDigits[r&0xf])
			start = i + size
		}
		i += size
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

func appendEscapedASCII(dst []byte, c byte) []byte {
	switch c {
	case '"', '\\':
		return append(dst, '\\', c)
	case '\n':
		return append(dst, '\\', 'n')
	case '\r':
		return append(dst, '\\', 'r')
	case '\t':
		return append(dst, '\\', 't')
	default:
		return append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
	}
}
