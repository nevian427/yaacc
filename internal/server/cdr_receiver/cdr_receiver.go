package cdr_receiver

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/nevian427/yaacc/internal/model"
	jww "github.com/spf13/jwalterweatherman"
)

// приёмник данных от станции
func CDRReceiver(ctx context.Context, inputCh chan<- string, dbCh chan model.CDR, CDRAddr string) (ErrCDR error) {
	var wg sync.WaitGroup

	// пытаемся понять куда цепляться
	ta, err := net.ResolveTCPAddr("tcp", CDRAddr)
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

	defer func() {
		err := ln.Close()
		if err != nil {
			if ErrCDR != nil {
				ErrCDR = fmt.Errorf("%w Can't close connection: %s", ErrCDR, err.Error())
			}
			ErrCDR = err
			return
		}

		wg.Wait()
		jww.INFO.Println("Stopped CDR listener")
		// закрываем каналы воркеров и БД для из завершения
		close(inputCh)
		close(dbCh)
		return
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
