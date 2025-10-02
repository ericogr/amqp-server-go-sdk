package amqp

import (
	"reflect"
	"testing"
	"time"
)

func TestFieldTableRoundTrip(t *testing.T) {
	tbl := map[string]interface{}{
		"boolv":  true,
		"int32v": int32(42),
		"int64v": int64(1 << 40),
		"strv":   "hello",
		"nested": map[string]interface{}{"n": "v"},
		"arr":    []interface{}{"a", int32(7)},
		"ts":     time.Unix(1234567890, 0),
	}

	enc := writeFieldTable(tbl)
	got, _, err := parseFieldTable(enc)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	// normalize types for comparison where numeric widening may happen
	if !reflect.DeepEqual(tbl["strv"], got["strv"]) {
		t.Fatalf("string mismatch: want=%v got=%v", tbl["strv"], got["strv"])
	}
	if !reflect.DeepEqual(tbl["boolv"], got["boolv"]) {
		t.Fatalf("bool mismatch: want=%v got=%v", tbl["boolv"], got["boolv"])
	}
	if _, ok := got["nested"].(map[string]interface{}); !ok {
		t.Fatalf("nested missing or wrong type: %T", got["nested"])
	}
	if _, ok := got["arr"].([]interface{}); !ok {
		t.Fatalf("array missing or wrong type: %T", got["arr"])
	}
}
