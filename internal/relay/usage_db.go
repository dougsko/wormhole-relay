// Package relay usage database handling. The usage schema under
// db-schemas/ is copied verbatim from the reference implementation and
// must not be edited.
package relay

import (
	"database/sql"
	"embed"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite" // pure-Go SQLite driver
)

//go:embed db-schemas
var usageSchemas embed.FS

const usageDBTargetVersion = 1

// UsageDBError indicates an unrecoverable usage database problem.
type UsageDBError struct {
	msg string
}

func (e *UsageDBError) Error() string { return e.msg }

func usageDBErrorf(format string, args ...interface{}) *UsageDBError {
	return &UsageDBError{msg: fmt.Sprintf(format, args...)}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// OpenUsageDB opens or creates the transit relay usage database. It mirrors
// database.get_db() in the Python reference (TARGET_VERSION=1, no upgrade
// chain). ":memory:" is supported for tests. The parent directory of a file
// path must exist.
func OpenUsageDB(dbfile string) (*sql.DB, error) {
	if dbfile == ":memory:" {
		db, err := openUsageDBConnection(dbfile)
		if err != nil {
			return nil, err
		}
		if err := initializeUsageDB(db); err != nil {
			db.Close()
			return nil, err
		}
		return db, nil
	}

	if fileExists(dbfile) {
		db, err := openUsageDBConnection(dbfile)
		if err != nil {
			return nil, err
		}
		var version int
		if err := db.QueryRow("SELECT version FROM version").Scan(&version); err != nil {
			db.Close()
			return nil, usageDBErrorf("unable to read db version: %v", err)
		}
		if version != usageDBTargetVersion {
			db.Close()
			return nil, usageDBErrorf("unable to handle db version %d", version)
		}
		return db, nil
	}

	if err := atomicCreateAndInitializeUsageDB(dbfile); err != nil {
		return nil, err
	}
	return openUsageDBConnection(dbfile)
}

func openUsageDBConnection(dbfile string) (*sql.DB, error) {
	dsn := dbfile
	if dbfile != ":memory:" {
		dsn = "file:" + dbfile + "?_pragma=foreign_keys(1)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, usageDBErrorf("unable to create/open db file %q: %v", dbfile, err)
	}
	// serialize all access; matches the Python implementation's
	// single-threaded reactor semantics and keeps PRAGMA behavior predictable
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, usageDBErrorf("unable to create/open db file %q: %v", dbfile, err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		db.Close()
		return nil, usageDBErrorf("failed to enable foreign keys: %v", err)
	}
	rows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		db.Close()
		return nil, usageDBErrorf("failed foreign key check: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		db.Close()
		return nil, usageDBErrorf("failed foreign key check")
	}
	return db, nil
}

func initializeUsageDB(db *sql.DB) error {
	schema, err := usageSchemas.ReadFile("db-schemas/usage-v1.sql")
	if err != nil {
		return usageDBErrorf("unable to read schema usage-v1: %v", err)
	}
	if _, err := db.Exec(string(schema)); err != nil {
		return usageDBErrorf("failed to initialize usage schema v%d: %v", usageDBTargetVersion, err)
	}
	if _, err := db.Exec("INSERT INTO version (version) VALUES (?)", usageDBTargetVersion); err != nil {
		return usageDBErrorf("failed to set schema version: %v", err)
	}
	return nil
}

func atomicCreateAndInitializeUsageDB(dbfile string) error {
	dir := filepath.Dir(dbfile)
	if dir == "" {
		dir = "."
	}
	fmt.Printf("populating new database with schema v%d\n", usageDBTargetVersion)

	tmp, err := os.CreateTemp(dir, filepath.Base(dbfile)+".*")
	if err != nil {
		return usageDBErrorf("unable to create temporary database near %q: %v", dbfile, err)
	}
	tmpName := tmp.Name()
	tmp.Close()
	defer os.Remove(tmpName) // no-op once renamed

	db, err := openUsageDBConnection(tmpName)
	if err != nil {
		return err
	}
	if err := initializeUsageDB(db); err != nil {
		db.Close()
		return err
	}
	db.Close()
	if err := os.Rename(tmpName, dbfile); err != nil {
		return usageDBErrorf("unable to rename %q to %q: %v", tmpName, dbfile, err)
	}
	return nil
}
