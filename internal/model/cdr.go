package model

import "time"

// Структура под конкретную конфигурацию пользовательской станции.
type CDR struct {
	Timestamp  time.Time `db:"datetime"`
	Ip         string    `db:"host"`
	CondCode   string    `db:"cond_code"`
	CodeDial   string    `db:"code_dial"`
	CodeUsed   string    `db:"code_used"`
	DialedNum  string    `db:"dst_num"`
	CallingNum string    `db:"src_num"`
	AcctCode   string    `db:"acct_code"`
	InTrk      string    `db:"in_trunk"`
	Vdn        string    `db:"vdn"`
	FeatFlag   string    `db:"feat_flag"`
	Duration   int       `db:"duration"`
	Ppm        int       `db:"ppm"`
	InTrkS     int       `db:"in_trunk_ts"`
	OutTrkS    int       `db:"out_trunk_ts"`
}
