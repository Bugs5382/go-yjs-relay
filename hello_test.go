package yjsrelay

import "testing"

func TestHello(t *testing.T) {
	got := Hello("world")
	want := "Hello, world!"
	if got != want {
		t.Errorf("Hello() = %q, want %q", got, want)
	}
}
