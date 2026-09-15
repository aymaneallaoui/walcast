package event

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
	"unicode/utf8"
)

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

func TestAppendStringRoundTrips(t *testing.T) {
	inputs := []string{
		"", "plain", `quote " and \ backslash`, "line\nfeed\r\ttab",
		"ctrl \x00\x01\x1f", "unicode é 日本語 🚀", `{"looks":"like json"}`, "</script>",
		"line sep \u2028 para sep \u2029 nel \u0085",
	}
	for _, in := range inputs {
		out := appendString(nil, []byte(in))

		var got string
		if err := json.Unmarshal(out, &got); err != nil {
			t.Fatalf("%q encoded to invalid JSON %s: %v", in, out, err)
		}
		if got != in {
			t.Errorf("round trip of %q gave %q", in, got)
		}
	}
}

func TestAppendValueByOID(t *testing.T) {
	cases := []struct {
		name string
		oid  uint32
		in   string
		want string
	}{
		{"bool true", oidBool, "t", "true"},
		{"bool false", oidBool, "f", "false"},
		{"int8", oidInt8, "-9223372036854775808", "-9223372036854775808"},
		{"float", oidFloat8, "1.5e+300", "1.5e+300"},
		{"float nan", oidFloat8, "NaN", `"NaN"`},
		{"float -inf", oidFloat4, "-Infinity", `"-Infinity"`},
		{"numeric keeps precision as string", oidNumeric, "0.10000000000000000001", `"0.10000000000000000001"`},
		{"jsonb raw", oidJSONB, `{"a": 1}`, `{"a": 1}`},
		{"json loses raw line breaks", oidJSON, "{\n  \"a\": \"x\\ny\"\r\n}", `{  "a": "x\ny"}`},
		{"timestamptz text", 1184, "2026-09-20 10:00:00+00", `"2026-09-20 10:00:00+00"`},
		{"int array stays pg text", 1007, "{1,2,3}", `"{1,2,3}"`},
		{"bytea hex", 17, `\xdeadbeef`, `"\\xdeadbeef"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := string(appendValue(nil, c.oid, []byte(c.in))); got != c.want {
				t.Errorf("got %s, want %s", got, c.want)
			}
		})
	}
}

func TestAppendStringReplacesInvalidUTF8(t *testing.T) {
	out := appendString(nil, []byte("a\xffb\xc3"))

	var got string
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("invalid JSON %s: %v", out, err)
	}
	if want := "a\ufffdb\ufffd"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func FuzzAppendString(f *testing.F) {
	for _, seed := range []string{"", "plain", "q\"\\", "\n\r\t\x00", "\u2028\u2029\u0085", "\xff\xfe", "日本語"} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, in []byte) {
		out := appendString(nil, in)
		if bytes.ContainsAny(out, "\n\r") || bytes.Contains(out, []byte("\u2028")) || bytes.Contains(out, []byte("\u2029")) {
			t.Fatalf("raw line break in %q", out)
		}
		var got string
		if err := json.Unmarshal(out, &got); err != nil {
			t.Fatalf("%q encoded to invalid JSON %q: %v", in, out, err)
		}
		if want := string(bytes.ToValidUTF8(in, []byte("\ufffd"))); utf8.Valid(in) && got != want {
			t.Fatalf("round trip of %q gave %q", in, got)
		}
	})
}

func FuzzAppendValueJSON(f *testing.F) {
	f.Add([]byte("{\n \"a\": [1, 2],\r\n \"b\": \"x\\ny\"}"))
	f.Fuzz(func(t *testing.T, in []byte) {
		if !json.Valid(in) {
			t.Skip()
		}
		out := appendValue(nil, oidJSON, in)
		if bytes.ContainsAny(out, "\n\r") {
			t.Fatalf("raw line break survived in %q", out)
		}
		if !json.Valid(out) {
			t.Fatalf("valid JSON %q became invalid %q", in, out)
		}
	})
}
