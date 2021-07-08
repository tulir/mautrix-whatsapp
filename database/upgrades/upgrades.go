package upgrades

import (
	"database/sql"
	"fmt"
	"strings"

	log "maunium.net/go/maulogger/v2"
)

type Dialect int

const (
	Postgres Dialect = iota
	SQLite
)

func (dialect Dialect) String() string {
	switch dialect {
	case Postgres:
		return "postgres"
	case SQLite:
		return "sqlite3"
	default:
		return ""
	}
}

type upgradeFunc func(*sql.Tx, context) error

type context struct {
	dialect Dialect
	db      *sql.DB
	log     log.Logger
}

type upgrade struct {
	message string
	fn      upgradeFunc
}

const NumberOfUpgrades = 22

var upgrades [NumberOfUpgrades]upgrade

var UnsupportedDatabaseVersion = fmt.Errorf("unsupported database version")

func GetVersion(db *sql.DB) (int, error) {
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS version (version INTEGER)")
	if err != nil {
		return -1, err
	}

	version := 0
	row := db.QueryRow("SELECT version FROM version LIMIT 1")
	if row != nil {
		_ = row.Scan(&version)
	}
	return version, nil
}

func SetVersion(tx *sql.Tx, version int) error {
	_, err := tx.Exec("DELETE FROM version")
	if err != nil {
		return err
	}
	_, err = tx.Exec("INSERT INTO version (version) VALUES ($1)", version)
	return err
}

func Run(log log.Logger, dialectName string, db *sql.DB) error {
	var dialect Dialect
	switch strings.ToLower(dialectName) {
	case "postgres":
		dialect = Postgres
	case "sqlite3":
		dialect = SQLite
	default:
		return fmt.Errorf("unknown dialect %s", dialectName)
	}

	version, err := GetVersion(db)
	if err != nil {
		return err
	}

	if version > NumberOfUpgrades {
		return UnsupportedDatabaseVersion
	}

	log.Infofln("Database currently on v%d, latest: v%d", version, NumberOfUpgrades)
	for i, upgrade := range upgrades[version:] {
		log.Infofln("Upgrading database to v%d: %s", version+i+1, upgrade.message)
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		err = upgrade.fn(tx, context{dialect, db, log})
		if err != nil {
			return err
		}
		err = SetVersion(tx, version+i+1)
		if err != nil {
			return err
		}
		err = tx.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}
