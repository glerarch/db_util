package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type PostgresDB struct {
	Username string
	Password string
}

type DBConfig struct {
	Host     string
	User     string
	Password string
	Port     string
	DBName   string
}

func (db PostgresDB) Connect(cfg *DBConfig) (*sql.DB, error) {
	if cfg == nil {
		return nil, errors.New("database config is required")
	}
	psqlURL := fmt.Sprintf("host=%s port=%s user=%s "+"password=%s dbname=%s sslmode=disable", cfg.Host, cfg.Host, "postgres", cfg.Password, cfg.DBName)
	return sql.Open("postgres", psqlURL)
}

type Operation func(ctx context.Context, ext sqlx.ExtContext) error

func doOnce(ctx context.Context, op Operation, db *sqlx.DB) error {
	tx, err := db.BeginTxx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := op(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func newExponentialBackOff() *backoff.ExponentialBackOff {
	b := &backoff.ExponentialBackOff{
		InitialInterval:     10 * time.Millisecond,
		RandomizationFactor: backoff.DefaultRandomizationFactor,
		Multiplier:          backoff.DefaultMultiplier,
		MaxInterval:         time.Second,
		MaxElapsedTime:      0, // retry for every
		Clock:               backoff.SystemClock,
	}
	b.Reset()
	return b
}
