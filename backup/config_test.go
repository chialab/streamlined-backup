package backup

import (
	"testing"
)

func TestListOfStrings(t *testing.T) {
	t.Parallel()

	l := listOfStrings{"foo", "bar"}
	if err := l.Set("baz"); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(l) != 3 {
		t.Fatalf("Unexpected length: %d", len(l))
	}
	if l[0] != "foo" || l[1] != "bar" || l[2] != "baz" {
		t.Fatalf("Unexpected list: %v", l)
	}
	if l.String() != "foo,bar,baz" {
		t.Fatalf("Unexpected list: %v", l)
	}
}
