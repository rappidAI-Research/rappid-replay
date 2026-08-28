package privacy

import (
	"reflect"
	"testing"
)

func TestRedactCommandArgs(t *testing.T) {
	input := []string{
		"agent",
		"--api-key", "supersecretvalue",
		"--token=anothersecret",
		"PASSWORD=hunter123",
		"--model", "token",
	}
	got, redacted := RedactCommandArgs(input)
	if !redacted {
		t.Fatal("RedactCommandArgs() did not report redaction")
	}
	want := []string{
		"agent",
		"--api-key", "[REDACTED]",
		"--token=[REDACTED]",
		"PASSWORD=[REDACTED]",
		"--model", "token",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RedactCommandArgs() = %#v, want %#v", got, want)
	}
	if input[2] != "supersecretvalue" || input[3] != "--token=anothersecret" {
		t.Fatalf("RedactCommandArgs() mutated input: %#v", input)
	}
}

func TestRedactCommandArgsDoesNotTreatExecutableNamedTokenAsFlag(t *testing.T) {
	input := []string{"token", "ordinary-argument"}
	got, redacted := RedactCommandArgs(input)
	if redacted || !reflect.DeepEqual(got, input) {
		t.Fatalf("RedactCommandArgs(%#v) = %#v, redacted=%v", input, got, redacted)
	}
}
