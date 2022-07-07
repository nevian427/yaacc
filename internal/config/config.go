package config

import (
	"fmt"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
	jww "github.com/spf13/jwalterweatherman"
	"github.com/spf13/pflag"
)

type Config struct {
	CDRAddr     string `toml:"cdraddr" env:"YAACC_CDRADDR" env-default:":5013"`
	MetricsAddr string `toml:"metricsaddr" env:"YAACC_METRICSADDR" env-default:":9013"`
	DBDriver    string `toml:"dbdriver" env:"YAACC_DBDRIVER" env-default:"pg"`
	DBPort      int    `toml:"dbport" env:"YAACC_DBPORT" env-default:"5432"`
	DBHost      string `toml:"dbhost" env:"YAACC_DBHOST" env-default:"localhost"`
	DBName      string `toml:"dbname" env:"YAACC_DBNAME" env-default:"postgres"`
	DBUser      string `toml:"dbuser" env:"YAACC_DBUSER" env-default:"cdr"`
	DBPassword  string `toml:"dbpassword" env:"YAACC_DBPASSWORD"`
	Logfile     string `toml:"logfile" env:"YAACC_LOGFILE"`
	FailCDRFile string `toml:"failcdr" env:"YAACC_FAILCDR"`
}

func ParseConfig() (*Config, error) {
	cfg := &Config{}
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
			return nil, err
		}
		defer f.Close()
		cfg.Logfile = *logfile
		// в консоль теперь сыпем только ошибки
		// jww.SetStdoutThreshold(jww.LevelError)
		// запись логов в файл
		jww.SetLogOutput(f)
	}

	// считываем конфиг
	err := cleanenv.ReadConfig(*cfgfile, cfg)
	// без конфига нам делать нечего - выход
	if err != nil {
		return nil, fmt.Errorf("Error loading config (%s): %q", *cfgfile, err)
	}
	return cfg, nil
}
