package storage

import (
	"context"
	"fmt"

	"github.com/nevian427/yaacc/internal/config"
	"github.com/nevian427/yaacc/internal/storage/contracts"
	"github.com/nevian427/yaacc/internal/storage/pg"
)

func New(ctx context.Context, cfg *config.Config) (contracts.IStore, error) {
	switch cfg.DBDriver {
	// case "mysql":
	// 	return mysql.Connect(cfg)
	case "pg", "pgsql", "postgresql":
		return pg.Connect(ctx, cfg)
	default:
		return nil, fmt.Errorf("%w: Invalid DB driver %s", contracts.ErrDBConnFail, cfg.DBDriver)
	}
}
