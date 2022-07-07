package ucase

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/nevian427/yaacc/internal/model"
	jww "github.com/spf13/jwalterweatherman"
)

// основная задача - разобрать чего же нам прислала телефонная станция
func decodeCDR(r string) (model.CDR, error) {
	// Нам нужен экземпляр структуры для правильного присвоения полей + ошибка
	var (
		cdr model.CDR
		err error
	)

	// если длина не совпадает, значит не тот формат на который мы заточены
	if len(r) != 114 {
		return model.CDR{}, fmt.Errorf("Incorrect line length(%d)", len(r))
	}

	loc, _ := time.LoadLocation("Local")

	// Заполняем длительность первой - она нам понадобится при вычислении старта звонка
	cdr.Duration, err = strconv.Atoi(r[12:17])
	if err != nil {
		return model.CDR{}, err
	}
	// Приводим авайский формат даты/времени окончания вызова к стандартному времени
	cdr.Timestamp, err = time.ParseInLocation("020106 1504", r[0:11], loc)
	if err != nil {
		return model.CDR{}, err
	}
	// Вычисляем реальное время начала вызова
	cdr.Timestamp = cdr.Timestamp.Add(-time.Second * time.Duration(cdr.Duration))
	cdr.CondCode = string(r[18])
	cdr.CodeDial = strings.TrimLeft(r[20:24], " ")
	cdr.CodeUsed = strings.TrimLeft(r[25:29], " ")
	cdr.DialedNum = strings.TrimLeft(r[30:53], " ")
	cdr.CallingNum = strings.TrimLeft(r[54:69], " ")
	cdr.AcctCode = strings.TrimLeft(r[70:85], " ")
	// здесь и далее игнорируем ошибки - поля могут быть пустыми
	cdr.Ppm, _ = strconv.Atoi(r[86:91])
	cdr.InTrkS, _ = strconv.Atoi(r[92:95])
	cdr.OutTrkS, _ = strconv.Atoi(r[96:99])
	cdr.InTrk = strings.TrimLeft(r[100:104], " ")
	cdr.Vdn = strings.TrimLeft(r[105:112], " ")
	cdr.FeatFlag = string(r[113])

	return cdr, nil
}

// рабочие лошадки
func WorkerCDR(inputCh <-chan string, dbCh, metricsCh chan<- model.CDR) error {
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
			// увеличиваем счётчик ошибок для конкретного источника
			metrics.GetOrCreateCounter("yaacc_cdr_err_count{source=\"" + data[:strings.Index(data, "|")] + "\"}").Add(1)

		} else {
			// выставляем источник сообщения
			cdr.Ip = data[:strings.Index(data, "|")]

			// fan-out в БД и метрики с некотрым таймаутом
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
