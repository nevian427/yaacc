package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/nevian427/yaacc/internal/config"
	"github.com/nevian427/yaacc/internal/model"
	"github.com/nevian427/yaacc/internal/storage/contracts"
	jww "github.com/spf13/jwalterweatherman"
)

type pgStore struct {
	*pgxpool.Pool
}

// Создаём новое соединение с БД
func Connect(ctx context.Context, cfg *config.Config) (contracts.IStore, error) {
	pool, err := pgxpool.Connect(ctx, fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable", cfg.DBHost, cfg.DBPort, cfg.DBName, cfg.DBUser, cfg.DBPassword))
	if err != nil {
		// без БД делать нам нечего - аварийно выходим
		return nil, fmt.Errorf("%w err: %s", contracts.ErrDBConnFail, err)
	}
	return &pgStore{pool}, nil
}

func (p *pgStore) CreateTable(ctx context.Context, table string) error {
	// Пытаемся создать таблицу для сохранения информации
	query := `CREATE TABLE IF NOT EXISTS ` + table +
		`(
      id serial primary key,
      host varchar(15) not null,
      datetime timestamp,
      duration int4,
      cond_code bpchar(1),
      code_dial bpchar(4),
      code_used bpchar(4),
      dst_num varchar(23) not null,
      src_num varchar(15),
      acct_code varchar(15),
      ppm int4,
      in_trunk_ts int4,
      out_trunk_ts int4,
      in_trunk bpchar(4),
      vdn varchar(7),
      feat_flag bpchar(1)
    );
    CREATE INDEX IF NOT EXISTS ` + table + `_code_used_idx ON public.` + table + ` USING btree (code_used);
    CREATE INDEX IF NOT EXISTS ` + table + `_dst_num_idx ON public.` + table + ` USING btree (dst_num);
    CREATE INDEX IF NOT EXISTS ` + table + `_in_trunk_idx ON public.` + table + ` USING btree (in_trunk);
    CREATE INDEX IF NOT EXISTS ` + table + `_src_num_idx ON public.` + table + ` USING btree (src_num);
    CREATE INDEX IF NOT EXISTS ` + table + `_vdn_idx ON public.` + table + ` USING btree (vdn);
    CREATE INDEX IF NOT EXISTS ` + table + `_datetime_idx ON public.` + table + ` USING brin (datetime)`

	_, err := p.Pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("%w err: %s", contracts.ErrDBCreateTable, err)
	}

	return nil
}

func (p *pgStore) insert(ctx context.Context, cdr model.CDR) error {
	// Exec сам возвращает соединение в пул
	commandTag, err := p.Pool.Exec(ctx, "insert into avaya_cdr (host, datetime, duration, cond_code, code_dial, code_used, dst_num, src_num, acct_code, ppm, in_trunk_ts, out_trunk_ts, in_trunk, vdn, feat_flag) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)", cdr.Ip, cdr.Timestamp, cdr.Duration, cdr.CondCode, cdr.CodeDial, cdr.CodeUsed, cdr.DialedNum, cdr.CallingNum, cdr.AcctCode, cdr.Ppm, cdr.InTrkS, cdr.OutTrkS, cdr.InTrk, cdr.Vdn, cdr.FeatFlag)
	if err != nil {
		jww.ERROR.Printf("CDR not inserted: %s", err)
		return err
	} else if commandTag.RowsAffected() != 1 {
		jww.ERROR.Println("CDR not inserted for unknown reason")
		return errors.New("CDR not inserted for unknown reason")
	}
	return nil
}

// Вставка разобраной строки из структуры в БД
func (p *pgStore) WatchCDR(ctx context.Context, dbCh <-chan model.CDR) error {
	defer func() {
		// заглушка, реальное закрытие в майне
		jww.INFO.Println("Shutdown insertDB worker")
	}()

	for {
		select {
		// сигнал на выход
		case <-ctx.Done():
			return ctx.Err()
		case cdr, ok := <-dbCh:
			// канал закрыт делать больше нечего
			if !ok {
				return nil
			}
			if err := p.insert(ctx, cdr); err != nil {
				return err
			}
		}
	}
}

func (p *pgStore) Close() {
	p.Pool.Close()
}
