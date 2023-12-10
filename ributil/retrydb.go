package ributil

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// RetryDB retries 'database is locked' errors
type RetryDB struct {
	db *sql.DB
}

func NewRetryDB(db *sql.DB) *RetryDB {
	return &RetryDB{db: db}
}

func isDbLockedError(err error) bool {
	return strings.Contains(err.Error(), "database is locked")
}

// Exec
func (d *RetryDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	var err error
	var result sql.Result
	for {
		result, err = d.db.Exec(query, args...)
		if err == nil {
			return result, nil
		}
		if !isDbLockedError(err) {
			return nil, err
		}

		log.Errorw("database is locked, retrying", "query", query, "args", args, "err", err)
		time.Sleep(50 * time.Millisecond)
	}
}

// ExecContext
func (d *RetryDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	var err error
	var result sql.Result
	for {
		result, err = d.db.ExecContext(ctx, query, args...)
		if err == nil {
			return result, nil
		}
		if !isDbLockedError(err) {
			return nil, err
		}

		log.Errorw("database is locked, retrying", "query", query, "args", args, "err", err)
		time.Sleep(50 * time.Millisecond)
	}
}

// QueryRow
func (d *RetryDB) QueryRow(query string, args ...interface{}) *sql.Row {
	var err error
	var row *sql.Row
	for {
		row = d.db.QueryRow(query, args...)
		err = row.Err()
		if err == nil {
			return row
		}
		if !isDbLockedError(err) {
			return row
		}

		log.Errorw("database is locked, retrying", "query", query, "args", args, "err", err)
		time.Sleep(50 * time.Millisecond)
	}
}

// Query
func (d *RetryDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	var err error
	var rows *sql.Rows
	for {
		rows, err = d.db.Query(query, args...)
		if err == nil {
			return rows, nil
		}
		if !isDbLockedError(err) {
			return nil, err
		}

		log.Errorw("database is locked, retrying", "query", query, "args", args, "err", err)
		time.Sleep(50 * time.Millisecond)
	}
}

// QueryContext
func (d *RetryDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	var err error
	var rows *sql.Rows
	for {
		rows, err = d.db.QueryContext(ctx, query, args...)
		if err == nil {
			return rows, nil
		}
		if !isDbLockedError(err) {
			return nil, err
		}

		log.Errorw("database is locked, retrying", "query", query, "args", args, "err", err)
		time.Sleep(50 * time.Millisecond)
	}
}

// Begin
func (d *RetryDB) Begin() (*sql.Tx, error) {
	// todo retryTx

	var err error
	var tx *sql.Tx
	for {
		tx, err = d.db.Begin()
		if err == nil {
			return tx, nil
		}
		if !isDbLockedError(err) {
			return nil, err
		}

		log.Errorw("database is locked, retrying", "err", err)
		time.Sleep(50 * time.Millisecond)
	}
}
