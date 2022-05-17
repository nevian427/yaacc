package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/oklog/run"
	jww "github.com/spf13/jwalterweatherman"
	"github.com/spf13/pflag"
)

type Config struct {
	CDRAddr     string `toml:"cdraddr" env:"YAACC_CDRADDR" env-default:":5013"`
	MetricsAddr string `toml:"metricsaddr" env:"YAACC_METRICSADDR" env-default:":9013"`
	DBPort      int    `toml:"dbport" env:"YAACC_DBPORT" env-default:"5432"`
	DBHost      string `toml:"dbhost" env:"YAACC_DBHOST" env-default:"localhost"`
	DBName      string `toml:"dbname" env:"YAACC_DBNAME" env-default:"postgres"`
	DBUser      string `toml:"dbuser" env:"YAACC_DBUSER" env-default:"cdr"`
	DBPassword  string `toml:"dbpassword" env:"YAACC_DBPASSWORD"`
	Logfile     string `toml:"logfile" env:"YAACC_LOGFILE"`
	FailCDRFile string `toml:"failcdr" env:"YAACC_FAILCDR"`
}

// Структура под конкретную конфигурацию пользовательской станции.
type CDR struct {
	timestamp  time.Time
	ip         string
	condCode   string
	codeDial   string
	codeUsed   string
	dialedNum  string
	callingNum string
	acctCode   string
	inTrk      string
	vdn        string
	featFlag   string
	duration   int
	ppm        int
	inTrkS     int
	outTrkS    int
}

var (
	cfg       Config
	dbCh      chan CDR
	metricsCh chan CDR
	inputCh   chan string
)

// Создаём новое соединение с БД
func connectToDB() *pgxpool.Pool {
	pool, err := pgxpool.Connect(context.Background(), fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable", cfg.DBHost, cfg.DBPort, cfg.DBName, cfg.DBUser, cfg.DBPassword))
	if err != nil {
		// без БД делать нам нечего - аварийно выходим
		jww.ERROR.Fatalf("DB connection failed: %s", err)
		return nil
	}

	// Пытаемся создать таблицу для сохранения информации
	_, err = pool.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS avaya_cdr
		(
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
		CREATE INDEX IF NOT EXISTS avaya_cdr_code_used_idx ON public.avaya_cdr USING btree (code_used);
		CREATE INDEX IF NOT EXISTS avaya_cdr_dst_num_idx ON public.avaya_cdr USING btree (dst_num);
		CREATE INDEX IF NOT EXISTS avaya_cdr_in_trunk_idx ON public.avaya_cdr USING btree (in_trunk);
		CREATE INDEX IF NOT EXISTS avaya_cdr_src_num_idx ON public.avaya_cdr USING btree (src_num);
		CREATE INDEX IF NOT EXISTS avaya_cdr_vdn_idx ON public.avaya_cdr USING btree (vdn);`)

	if err != nil {
		jww.ERROR.Fatalf("Creation CDR table failed: %s", err)
		return nil
	}

	jww.INFO.Println("Started DB pool")
	return pool
}

// Вставка разобраной строки из структуры в БД
func insertDB(pool *pgxpool.Pool) error {
	defer func() {
		// заглушка, реальное закрытие в майне
		jww.INFO.Println("closing DB connection")
	}()

	for {
		cdr, ok := <-dbCh
		// канал закрыт делать больше нечего
		if !ok {
			return nil
		}

		// Exec сам возвращает соединение в пул
		commandTag, err := pool.Exec(context.Background(), "insert into avaya_cdr (host, datetime, duration, cond_code, code_dial, code_used, dst_num, src_num, acct_code, ppm, in_trunk_ts, out_trunk_ts, in_trunk, vdn, feat_flag) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)", cdr.ip, cdr.timestamp, cdr.duration, cdr.condCode, cdr.codeDial, cdr.codeUsed, cdr.dialedNum, cdr.callingNum, cdr.acctCode, cdr.ppm, cdr.inTrkS, cdr.outTrkS, cdr.inTrk, cdr.vdn, cdr.featFlag)
		if err != nil {
			jww.ERROR.Printf("CDR not inserted: %s", err)
		} else {
			if commandTag.RowsAffected() != 1 {
				jww.ERROR.Println("CDR not inserted for unknown reason")
			}
		}
	}
}

// собираем метрики во время работы
func metricsCDR() error {
	for {
		cdr, ok := <-metricsCh
		// канал закрыт делать больше нечего
		if !ok {
			return nil
		}
		metrics.GetOrCreateCounter("yaacc_cdr_count{source=\"" + cdr.ip + "\"}").Add(1)
		if cdr.vdn != "" {
			metrics.GetOrCreateCounter("yaacc_cdr_vdn_count{source=\"" + cdr.ip + "\", trunk=\"" + cdr.inTrk + "\", vdn=\"" + cdr.vdn + "\"}").Add(1)
			metrics.GetOrCreateCounter("yaacc_cdr_vdn_duration{source=\"" + cdr.ip + "\", trunk=\"" + cdr.inTrk + "\", vdn=\"" + cdr.vdn + "\"}").Add(cdr.duration)
		}
		if cdr.codeUsed != "" {
			metrics.GetOrCreateCounter("yaacc_cdr_trunk_count{source=\"" + cdr.ip + "\", trunk=\"" + cdr.codeUsed + "\", direction=\"outbound\"}").Add(1)
			metrics.GetOrCreateCounter("yaacc_cdr_trunk_duration{source=\"" + cdr.ip + "\", trunk=\"" + cdr.codeUsed + "\", direction=\"outbound\"}").Add(cdr.duration)
		}
		if cdr.inTrk != "" {
			metrics.GetOrCreateCounter("yaacc_cdr_trunk_count{source=\"" + cdr.ip + "\", trunk=\"" + cdr.inTrk + "\", direction=\"inbound\"}").Add(1)
			metrics.GetOrCreateCounter("yaacc_cdr_trunk_duration{source=\"" + cdr.ip + "\", trunk=\"" + cdr.inTrk + "\", direction=\"inbound\"}").Add(cdr.duration)
		}
	}
}

// основная задача - разобрать чего же нам прислала телефонная станция
func decodeCDR(r string) (CDR, error) {
	var (
		cdr CDR
		err error
	)

	// если длина не совпадает, значит не тот формат на который мы заточены
	if len(r) != 114 {
		return CDR{}, fmt.Errorf("Incorrect line length(%d)", len(r))
	}

	loc, _ := time.LoadLocation("Local")

	cdr.duration, err = strconv.Atoi(r[12:17])
	cdr.timestamp, err = time.ParseInLocation("020106 1504", r[0:11], loc)
	cdr.timestamp = cdr.timestamp.Add(-time.Second * time.Duration(cdr.duration))
	cdr.condCode = string(r[18])
	cdr.codeDial = strings.TrimLeft(r[20:24], " ")
	cdr.codeUsed = strings.TrimLeft(r[25:29], " ")
	cdr.dialedNum = strings.TrimLeft(r[30:53], " ")
	cdr.callingNum = strings.TrimLeft(r[54:69], " ")
	cdr.acctCode = strings.TrimLeft(r[70:85], " ")
	// здесь и далее игнорируем ошибки - поля могут быть пустыми
	cdr.ppm, _ = strconv.Atoi(r[86:91])
	cdr.inTrkS, _ = strconv.Atoi(r[92:95])
	cdr.outTrkS, _ = strconv.Atoi(r[96:99])
	cdr.inTrk = strings.TrimLeft(r[100:104], " ")
	cdr.vdn = strings.TrimLeft(r[105:112], " ")
	cdr.featFlag = string(r[113])
	if err != nil {
		return CDR{}, err
	}

	return cdr, nil
}

// приёмник данных от станции
func serveCDR(ctx context.Context) error {
	var wg sync.WaitGroup

	// пытаемся понять куда цепляться
	ta, err := net.ResolveTCPAddr("tcp", cfg.CDRAddr)
	if err != nil {
		// возвращаем ошибку и раннер потушит остальное ибо бессмысленно продолжать
		return err
	}

	// слушаем порт
	ln, err := net.ListenTCP("tcp", ta)
	if err != nil {
		// возвращаем ошибку и раннер потушит остальное
		return err
	}
	jww.INFO.Printf("Started CDR listener on %s", ln.Addr())

	defer func() error {
		err := ln.Close()
		if err != nil {
			return err
		}

		wg.Wait()
		jww.INFO.Println("Stopped CDR listener")
		// закрываем каналы воркеров и БД для из завершения
		close(inputCh)
		close(dbCh)
		return nil
	}()

	for {
		select {
		// получен сигнал на выход
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// если слишком долгое соединение
		if err := ln.SetDeadline(time.Now().Add(time.Second)); err != nil {
			return err
		}

		conn, err := ln.Accept()
		// не получилось принять соединение, сообщаем и ждём новое
		if err != nil {
			if opErr, ok := err.(*net.OpError); !ok || !opErr.Timeout() {
				jww.ERROR.Printf("failed to accept CDR connection: %v", err.Error())
			}
			continue
		}

		// обработка входящего соединения
		wg.Add(1)
		go func() {
			defer func() {
				jww.INFO.Printf("closing CDR connection from %s", conn.RemoteAddr())
				err := conn.Close()
				if err != nil {
					jww.ERROR.Println(err)
				}
				wg.Done()
			}()

			jww.INFO.Printf("new CDR connection %s -> %s", conn.RemoteAddr(), conn.LocalAddr())

			r := bufio.NewReader(conn)

			// цикл приёма данных
			for {
				buf, _, err := r.ReadLine()
				if err != nil {
					if err == io.EOF {
						return
					}
					jww.WARN.Printf("Error handling CDR connection %s\n%s", conn.RemoteAddr(), err)
					return
				}
				// достаём адрес и передаём его вместе с данными в воркеры
				srcIP, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
				inputCh <- srcIP + "|" + strings.TrimLeft(string(buf), " \f\t\v\000")
			}
		}()
	}
}

// рабочие лошадки
func workerCDR() error {
	for {
		data, ok := <-inputCh
		// канал закрыт - на выход
		if !ok {
			return nil
		}

		// парсим без адреса
		cdr, err := decodeCDR(data[strings.Index(data, "|")+1:])
		// не всегда удачно
		if err != nil {
			// выводим плохую строчку в лог
			jww.WARN.Printf("Error %s while parsing cdr\n\"%s\"", err, data[strings.Index(data, "|")+1:])
			err = nil
			// увеличиваем счётчик ошибок для конкретного источника
			metrics.GetOrCreateCounter("yaacc_cdr_err_count{source=\"" + data[:strings.Index(data, "|")] + "\"}").Add(1)

		} else {
			// выставляем источник сообщения
			cdr.ip = data[:strings.Index(data, "|")]

			// fan-out в БД и метрики с некоторым таймаутом
			lastWarn := time.Now()
			dbCh <- cdr
			if time.Since(lastWarn) > time.Second {
				jww.WARN.Println("overflowing DB channel")
			}
			lastWarn = time.Now()
			metricsCh <- cdr
			if time.Since(lastWarn) > time.Second {
				jww.WARN.Println("overflowing metrics channel")
			}
		}
	}
}

// публикация метрик
func serveMetrics(ctx context.Context) error {
	// создаём структуру сервера только для возможности последующего Shutdown
	srv := &http.Server{
		Addr: cfg.MetricsAddr,
	}

	// стандартный заголовок кодировки, но нужен ли он?
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8") // normal header
		metrics.WritePrometheus(w, true)
	})

	// Пытаемся слушать порт, но если не получилось - проживём без метрик
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			jww.WARN.Printf("Metrics listen err:%+s\n", err)
			return nil
		}
		jww.INFO.Printf("Metrics server started on %s", cfg.MetricsAddr)
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

func main() {
	var g run.Group
	// открываем каналы для работы
	// почему буфер 5 - уже не помню, какая-то эвристика..
	// запись в БД (из обработчика)
	dbCh = make(chan CDR, 5)
	// в метрики
	metricsCh = make(chan CDR, 5)
	// в обработчик
	inputCh = make(chan string, 5)

	// парсим флаги - нас интересуют только лог и конфиг
	cfgfile := pflag.StringP("config", "c", "./yaacc.toml", "File to read config params from")
	logfile := pflag.StringP("logfile", "l", "", "File to write exec time messages.")
	pflag.Lookup("logfile").NoOptDefVal = "./yaacc.log"
	pflag.Parse()

	// префикс для логирования
	jww.SetPrefix("yaacc")
	// по-умолчанию в файл пишем всё
	jww.SetLogThreshold(jww.LevelTrace)
	// в консоль поменьше подробностей
	jww.SetStdoutThreshold(jww.LevelInfo)

	// если нам сказали в лог - пишем туда
	if len(*logfile) > 0 {
		f, err := os.OpenFile(*logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			jww.ERROR.Fatal(err)
		}
		defer f.Close()
		cfg.Logfile = *logfile
		// в консоль теперь сыпем только ошибки
		jww.SetStdoutThreshold(jww.LevelError)
		// запись логов в файл
		jww.SetLogOutput(f)
	}

	// считываем конфиг
	err := cleanenv.ReadConfig(*cfgfile, &cfg)
	// без конфига нам делать нечего - выход
	if err != nil {
		jww.WARN.Fatalf("Error loading config (%s): %q", *cfgfile, err)
	}

	// spew.Dump(cfg)

	// инициализация пула соединений с БД
	pool := connectToDB()

	// основной контекст для работы - он может закрыть всё
	ctx, cancel := context.WithCancel(context.Background())

	// запускаем минимум по 4 копии
	nc := runtime.NumCPU()
	if nc < 4 {
		nc = 4
	}
	for n := 0; n < nc; n++ {
		// БД
		g.Add(func() error { return insertDB(pool) }, func(err error) {})
		// обработчик
		g.Add(func() error { return workerCDR() }, func(err error) {})
	}
	// обработчик метрик, на выходе закрыввем канал
	g.Add(func() error { return metricsCDR() }, func(err error) { close(metricsCh) })
	// приёмник CDR с отменой контекста по ошибке
	g.Add(func() error { return serveCDR(ctx) }, func(err error) { cancel() })
	// публикация метрик с отменой контекста по ошибке
	g.Add(func() error { return serveMetrics(ctx) }, func(err error) { cancel() })
	// перехват сигналов ОС с отменой контекста
	g.Add(run.SignalHandler(ctx, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL))
	// запускаем всё, при ошибке в любой компоненте - выход
	jww.INFO.Printf("Exit with: %v", g.Run())
	// закрываем БД
	pool.Close()
}
