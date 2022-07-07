package main

import (
	"context"
	_ "net/http/pprof"
	"runtime"
	"syscall"

	"github.com/nevian427/yaacc/internal/config"
	"github.com/nevian427/yaacc/internal/model"
	"github.com/nevian427/yaacc/internal/server/cdr_receiver"
	"github.com/nevian427/yaacc/internal/server/metrics"
	"github.com/nevian427/yaacc/internal/storage"
	"github.com/nevian427/yaacc/internal/ucase"
	"github.com/oklog/run"
	jww "github.com/spf13/jwalterweatherman"
)

func main() {
	cfg, err := config.ParseConfig()
	if err != nil {
		jww.ERROR.Fatal(err)
	}

	var (
		g run.Group
	)

	// открываем каналы для работы
	// почему буфер 5 - уже не помню, какая-то эвристика..
	// запись в БД (из обработчика)
	saveCh := make(chan model.CDR, 5)
	// в метрики
	metricsCh := make(chan model.CDR, 5)
	// в обработчик
	receiverCh := make(chan string, 5)

	// spew.Dump(cfg)

	// основной контекст для работы - он может закрыть всё
	ctx, cancel := context.WithCancel(context.Background())

	// инициализация пула соединений с БД
	db, err := storage.New(ctx, cfg)
	if err != nil {
		jww.WARN.Fatal(err)
	}

	// запускаем минимум по 4 копии
	nc := runtime.NumCPU()
	if nc < 4 {
		nc = 4
	}

	jww.INFO.Printf("Starting %d DB and CDR workers", nc)
	for n := 0; n < nc; n++ {
		// БД
		g.Add(func() error { return db.WatchCDR(ctx, saveCh) }, func(err error) {})
		// обработчик
		g.Add(func() error { return ucase.WorkerCDR(receiverCh, saveCh, metricsCh) }, func(err error) {})
	}
	// обработчик метрик, на выходе закрываем канал
	g.Add(func() error { return metrics.MetricsCDR(metricsCh) }, func(err error) { close(metricsCh) })
	// приёмник CDR с отменой контекста по ошибке
	g.Add(func() error { return cdr_receiver.CDRReceiver(ctx, receiverCh, saveCh, cfg.CDRAddr) }, func(err error) { cancel() })
	// публикация метрик с отменой контекста по ошибке
	g.Add(func() error { return metrics.ServeMetrics(ctx, cfg.MetricsAddr) }, func(err error) { cancel() })
	// перехват сигналов ОС с отменой контекста
	g.Add(run.SignalHandler(ctx, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL))
	// запускаем всё, при ошибке в любой компоненте - выход
	jww.INFO.Printf("Exit with: %v", g.Run())
	// закрываем БД
	db.Close()
}
