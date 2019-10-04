// +build !race

package redis

import (
	"bytes"
	"strings"
	"testing"
)

func TestNoAllocation(t *testing.T) {
	// both too large for stack:
	key := randomKey(strings.Repeat("k", 10e6))
	value := bytes.Repeat([]byte{'v'}, 10e6)

	f := func() {
		if err := testClient.SELECT(2); err != nil {
			t.Fatal(err)
		}

		if _, err := testClient.INCR(key); err != nil {
			t.Fatal(err)
		}
		if err := testClient.SET(key, value); err != nil {
			t.Fatal(err)
		}
		if _, err := testClient.APPEND(key, value); err != nil {
			t.Fatal(err)
		}
		if _, err := testClient.DEL(key); err != nil {
			t.Fatal(err)
		}

		if err := testClient.HMSET(key, []string{key}, [][]byte{value}); err != nil {
			t.Fatal(err)
		}
		if _, err := testClient.DELArgs(key); err != nil {
			t.Fatal(err)
		}
	}

	perRun := testing.AllocsPerRun(1, f)
	if perRun != 0 {
		t.Errorf("did %f memory allocations, want 0", perRun)
	}
}
