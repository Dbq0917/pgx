package pgx

import (
	"strings"
	"testing"
)

func TestLineScannerHandlesLongLinesAndLineEndings(t *testing.T) {
	longLine := strings.Repeat("x", 200*1024) // far larger than the 64KiB reader buffer
	input := "header\r\n" + longLine + "\nlast-without-newline"

	scanner := newLineScanner(strings.NewReader(input))

	var lines []string
	for scanner.Scan() {
		lines = append(lines, string(scanner.Bytes()))
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("unexpected scanner error: %v", err)
	}

	expected := []string{"header", longLine, "last-without-newline"}
	if len(lines) != len(expected) {
		t.Fatalf("expected %d lines, got %d", len(expected), len(lines))
	}
	for i := range expected {
		if lines[i] != expected[i] {
			t.Fatalf("line %d mismatch: got length %d, want %d", i, len(lines[i]), len(expected[i]))
		}
	}
}

func TestLineScannerEmptyInput(t *testing.T) {
	scanner := newLineScanner(strings.NewReader(""))
	if scanner.Scan() {
		t.Fatal("empty input must not produce a line")
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
