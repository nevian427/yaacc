## Yet Another Avaya CDR Collector
#### *Configuration options*

    CDRAddr     string `toml:"cdraddr" env:"YAACC_CDRADDR" env-default:":5013"`
    MetricsAddr string `toml:"metricsaddr" env:"YAACC_METRICSADDR" env-default:":9013"`
    DBPort      int    `toml:"dbport" env:"YAACC_DBPORT" env-default:"5432"`
    DBHost      string `toml:"dbhost" env:"YAACC_DBHOST" env-default:"localhost"`
    DBName      string `toml:"dbname" env:"YAACC_DBNAME" env-default:"postgres"`
    DBUser      string `toml:"dbuser" env:"YAACC_DBUSER" env-default:"cdr"`
    DBPassword  string `toml:"dbpassword" env:"YAACC_DBPASSWORD"`
    Logfile     string `toml:"logfile" env:"YAACC_LOGFILE"`
    FailCDRFile string `toml:"failcdr" env:"YAACC_FAILCDR"`
    
#### Expected CDR format 114 bytes long
![avaya custom format CDR](Screenshot 2021-07-23 at 16.57.52.png)
