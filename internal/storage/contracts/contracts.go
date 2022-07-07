package contracts

import (
	"context"
	"errors"

	"github.com/nevian427/yaacc/internal/model"
)

type IStore interface {
	CreateTable(ctx context.Context, table string) error
	// Insert(ctx context.Context, cdr model.CDR) error
	WatchCDR(ctx context.Context, dbCh <-chan model.CDR) error
	Close()
}

var (
	ErrDBConnFail    = errors.New("DB connection failed:")
	ErrDBCreateTable = errors.New("Creation CDR table failed:")
)
