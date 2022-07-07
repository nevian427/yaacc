package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/VictoriaMetrics/metrics"
	jww "github.com/spf13/jwalterweatherman"
)

// публикация метрик
func ServeMetrics(ctx context.Context, MetricsAddr string) error {
	// создаём структуру сервера только для возможности последующего Shutdown
	srv := &http.Server{
		Addr: MetricsAddr,
	}

	// стандартный заголовок кодировки, но нужен ли он?
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8") // normal header
		metrics.WritePrometheus(w, true)
	})

	// Пытаемся слушать порт, но если не получилось - проживём без метрик
	go func() {
		jww.INFO.Printf("Starting metrics server on %s", MetricsAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			jww.WARN.Printf("Metrics listen err:%+s\n", err)
		}
	}()

	// ждём сигнала завершения
	<-ctx.Done()

	// даём команду серверу завершиться в течение 5 сек иначе прибьём сами
	ctxShutDown, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer func() {
		cancel()
	}()

	if err := srv.Shutdown(ctxShutDown); err != nil && err != http.ErrServerClosed {
		// по-хорошему не вышло
		jww.WARN.Printf("Metrics Shutdown Failed:%+s", err)
	}

	jww.INFO.Printf("Metrics server stopped")

	return nil
}
