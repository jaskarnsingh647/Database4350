// kvstore - persistent key-value store using an append-only log (data.db).
// Commands (via STDIN):
//
//	SET <key> <value>
//	GET <key>
//	EXIT
package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ---- In-memory index (linked list, no built-in map) ----

type entry struct {
	key   string
	value string
	next  *entry
}

type kvIndex struct {
	head *entry
}

func (idx *kvIndex) set(key, value string) {
	for cur := idx.head; cur != nil; cur = cur.next {
		if cur.key == key {
			cur.value = value
			return
		}
	}
	idx.head = &entry{key: key, value: value, next: idx.head}
}

func (idx *kvIndex) get(key string) (string, bool) {
	for cur := idx.head; cur != nil; cur = cur.next {
		if cur.key == key {
			return cur.value, true
		}
	}
	return "", false
}

// ---- Persistence ----

// replayLog rebuilds the index by scanning the log from the beginning.
// Last write wins: we scan the whole file so later entries overwrite earlier ones.
func replayLog(idx *kvIndex, dbPath string) error {
	f, err := os.Open(dbPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 3)
		if len(parts) == 3 && parts[0] == "SET" {
			idx.set(parts[1], parts[2])
		}
	}
	return scanner.Err()
}

// appendSet writes a log entry and closes/reopens the file to force OS flush.
func appendSet(dbPath, key, value string) error {
	f, err := os.OpenFile(dbPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(f, "SET %s %s\n", key, value)
	if err != nil {
		f.Close()
		return err
	}
	// Close immediately — closing the file handle guarantees the OS
	// flushes all pending writes to disk before returning.
	return f.Close()
}

func dbPath() string {
	if len(os.Args) > 1 {
		return filepath.Join(os.Args[1], "data.db")
	}
	exe, err := os.Executable()
	if err == nil {
		real, err := filepath.EvalSymlinks(exe)
		if err == nil {
			exe = real
		}
		return filepath.Join(filepath.Dir(exe), "data.db")
	}
	return "data.db"
}

// ---- Main ----

func main() {
	path := dbPath()

	idx := &kvIndex{}
	if err := replayLog(idx, path); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR replaying log: %v\n", err)
		os.Exit(1)
	}

	scanner := bufio.NewScanner(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "SET":
			if len(parts) != 3 {
				fmt.Fprintln(writer, "ERROR: usage: SET <key> <value>")
			} else if err := appendSet(path, parts[1], parts[2]); err != nil {
				fmt.Fprintf(writer, "ERROR: %v\n", err)
			} else {
				idx.set(parts[1], parts[2])
				fmt.Fprintln(writer, "OK")
			}

		case "GET":
			if len(parts) != 2 {
				fmt.Fprintln(writer, "ERROR: usage: GET <key>")
			} else if value, ok := idx.get(parts[1]); ok {
				fmt.Fprintln(writer, value)
			} else {
				fmt.Fprintln(writer, "")
			}

		case "EXIT":
			writer.Flush()
			os.Exit(0)

		default:
			fmt.Fprintf(writer, "ERROR: unknown command %q\n", parts[0])
		}

		writer.Flush()
	}

	writer.Flush()
}
