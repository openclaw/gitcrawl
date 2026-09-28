package portable

import (
	"context"
	"path/filepath"
	"testing"
)

func TestExportPreservesOriginalLengthsWhenReexporting(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	source := filepath.Join(root, "source.db")
	st := seedExportSource(t, ctx, source)
	defer st.Close()
	options := testExportOptions(source, filepath.Join(root, "first"))
	options.BodyChars = 8
	first, err := Export(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	options.SourceDBPath = first.DatabasePath
	options.OutputDir = filepath.Join(root, "second")
	second, err := Export(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range []ExportResult{first, second} {
		db := openRawDB(t, result.DatabasePath)
		var threadLength, commentLength int
		if err := db.QueryRow(`select body_length from threads where id=1`).Scan(&threadLength); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`select body_length from comments where id=1`).Scan(&commentLength); err != nil {
			t.Fatal(err)
		}
		db.Close()
		if threadLength != 26 || commentLength != len("comment-current-body") {
			t.Errorf("export original lengths=%d,%d", threadLength, commentLength)
		}
	}
}
