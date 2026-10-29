package banner

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestPrint(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	Print("api", "1.0.0")

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	output := buf.String()

	if !strings.Contains(output, "UNYXDEV") {
		t.Errorf("expected banner to contain UNYXDEV, got: %s", output)
	}
	if !strings.Contains(output, "CONVERSATION ENGINE") {
		t.Errorf("expected banner to contain CONVERSATION ENGINE, got: %s", output)
	}
	if !strings.Contains(output, "api") {
		t.Errorf("expected banner to contain service name api, got: %s", output)
	}
}
