package jsonpath

import (
	"encoding/json"
	"testing"
)

func TestGet(t *testing.T) {
	var doc any
	_ = json.Unmarshal([]byte(`{"a":{"b":[{"c":7},{"c":"x"}]},"n":3}`), &doc)
	cases := []struct {
		path string
		ok   bool
		want string
	}{
		{"$.n", true, "3"},
		{"n", true, "3"},
		{"$.a.b[0].c", true, "7"},
		{"$.a.b.1.c", true, "x"},
		{"$.a.b[2]", false, ""},
		{"$.missing", false, ""},
	}
	for _, c := range cases {
		v, ok := Get(doc, c.path)
		if ok != c.ok || (ok && String(v) != c.want) {
			t.Errorf("%s: got %v,%v want %v,%q", c.path, v, ok, c.ok, c.want)
		}
	}
}

func TestFloat(t *testing.T) {
	if f, ok := Float(float64(4)); !ok || f != 4 {
		t.Error("float64")
	}
	if f, ok := Float("2.5"); !ok || f != 2.5 {
		t.Error("string")
	}
	if _, ok := Float([]any{}); ok {
		t.Error("slice should not convert")
	}
}
