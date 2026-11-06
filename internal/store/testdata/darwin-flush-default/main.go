package main

import (
	"database/sql"
	"log"

	_ "github.com/openclaw/gitcrawl/internal/store"
)

func main() {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	for _, pragma := range []string{"fullfsync", "checkpoint_fullfsync"} {
		var got int
		if err := db.QueryRow("PRAGMA " + pragma).Scan(&got); err != nil || got != 1 {
			log.Fatalf("%s=%d err=%v; want ON in a non-test binary", pragma, got, err)
		}
	}
}
