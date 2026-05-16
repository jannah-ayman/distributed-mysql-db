package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// openDB opens a connection to MySQL (no specific database selected yet)
func openDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

// createDatabase creates a new MySQL database
func createDatabase(db *sql.DB, dbName string) error {
	_, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName))
	return err
}

// dropDatabase drops a MySQL database
func dropDatabase(db *sql.DB, dbName string) error {
	_, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
	return err
}

// createTable creates a table with the given columns
// columns is a map of column name → MySQL type, e.g. {"name": "VARCHAR(100)", "age": "INT"}
// an auto-increment `id` column is always added as the primary key
func createTable(db *sql.DB, dbName, table string, columns map[string]string) error {

	parts := []string{"`id` INT AUTO_INCREMENT PRIMARY KEY"}

	for col, colType := range columns {
		parts = append(parts, fmt.Sprintf("`%s` %s", col, colType))
	}

	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s)",
		dbName, table, strings.Join(parts, ", "),
	)

	_, err := db.Exec(query)
	return err
}

// dropTable drops a table
func dropTable(db *sql.DB, dbName, table string) error {
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", dbName, table))
	return err
}

// insertRow inserts a row into the given table
// data is a map of column name → value
func insertRow(db *sql.DB, dbName, table string, data map[string]any) error {

	cols := make([]string, 0, len(data))
	placeholders := make([]string, 0, len(data))
	values := make([]any, 0, len(data))

	for col, val := range data {
		cols = append(cols, fmt.Sprintf("`%s`", col))
		placeholders = append(placeholders, "?")
		values = append(values, val)
	}

	query := fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
		dbName, table,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)

	_, err := db.Exec(query, values...)
	return err
}

// selectRows runs a SELECT on the given table with an optional WHERE condition,
// returns rows as a slice of maps
func selectRows(db *sql.DB, dbName, table, condition string) ([]map[string]any, error) {

	query := fmt.Sprintf("SELECT * FROM `%s`.`%s`", dbName, table)
	if condition != "" {
		query += " WHERE " + condition
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]any

	for rows.Next() {

		// make a slice of any to scan into
		values := make([]any, len(cols))
		valuePtrs := make([]any, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]any)
		for i, col := range cols {
			val := values[i]
			// convert []byte to string for readability
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}

		result = append(result, row)
	}

	return result, nil
}

// updateRows runs an UPDATE with SET data and optional WHERE condition
func updateRows(db *sql.DB, dbName, table string, data map[string]any, condition string) error {

	setClauses := make([]string, 0, len(data))
	values := make([]any, 0, len(data))

	for col, val := range data {
		setClauses = append(setClauses, fmt.Sprintf("`%s` = ?", col))
		values = append(values, val)
	}

	query := fmt.Sprintf(
		"UPDATE `%s`.`%s` SET %s",
		dbName, table, strings.Join(setClauses, ", "),
	)

	if condition != "" {
		query += " WHERE " + condition
	}

	_, err := db.Exec(query, values...)
	return err
}

// deleteRows runs a DELETE with optional WHERE condition
func deleteRows(db *sql.DB, dbName, table, condition string) error {

	query := fmt.Sprintf("DELETE FROM `%s`.`%s`", dbName, table)
	if condition != "" {
		query += " WHERE " + condition
	}

	_, err := db.Exec(query)
	return err
}
func upsertRow(db *sql.DB, dbName, table string, data map[string]any) error {
	cols := make([]string, 0, len(data))
	placeholders := make([]string, 0, len(data))
	updates := make([]string, 0, len(data))
	values := make([]any, 0, len(data))

	for col, val := range data {
		cols = append(cols, fmt.Sprintf("`%s`", col))
		placeholders = append(placeholders, "?")
		values = append(values, val)
		if col != "id" {
			updates = append(updates, fmt.Sprintf("`%s` = VALUES(`%s`)", col, col))
		}
	}

	query := fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		dbName, table,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(updates, ", "),
	)

	_, err := db.Exec(query, values...)
	return err
}
