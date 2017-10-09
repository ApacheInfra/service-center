package core

import (
	"flag"
	"github.com/ServiceComb/service-center/pkg/grace"
	"github.com/ServiceComb/service-center/pkg/lager"
	"github.com/ServiceComb/service-center/pkg/logrotate"
	"github.com/ServiceComb/service-center/pkg/tlsutil"
	"github.com/ServiceComb/service-center/pkg/util"
	"github.com/astaxie/beego"
	"os"
	"path/filepath"
	"time"
)

func init() {
	Initialize()
}

func Initialize() {
	initCommandLine()
	initLogger()
	tlsutil.LoadServerSSLConfig()
	tlsutil.LoadClientSSLConfig()
	initLogRotate()
	grace.Init()
}

func initCommandLine() {
	flag.CommandLine.Init(os.Args[0], flag.ContinueOnError)
	flag.CommandLine.Parse(os.Args[1:])
}

func initLogger() {
	logFormatText, err := beego.AppConfig.Bool("LogFormatText")
	loggerFile := os.ExpandEnv(beego.AppConfig.String("logfile"))
	loggerName := beego.AppConfig.String("ComponentName")
	enableRsyslog, err := beego.AppConfig.Bool("EnableRsyslog")
	if err != nil {
		enableRsyslog = false
	}

	enableStdOut := beego.AppConfig.DefaultString("runmode", "prod") == "dev"
	util.InitLogger(loggerName, &lager.Config{
		LoggerLevel:   beego.AppConfig.String("loglevel"),
		LoggerFile:    loggerFile,
		EnableRsyslog: enableRsyslog,
		LogFormatText: logFormatText,
		EnableStdOut:  enableStdOut,
	})

	// custom loggers
	util.CustomLogger("Heartbeat", "heartbeat")
	util.CustomLogger("HeartbeatSet", "heartbeat")

	util.CustomLogger("github.com/ServiceComb/service-center/server/service/event", "event")
	util.CustomLogger("github.com/ServiceComb/service-center/server/service/notification", "event")

	util.CustomLogger("github.com/ServiceComb/service-center/server/core/registry", "registry")
}

func initLogRotate() {
	logDir := os.ExpandEnv(beego.AppConfig.String("logfile"))
	rotatePeriod := 30 * time.Second
	maxFileSize := beego.AppConfig.DefaultInt("log_rotate_size", 20)
	if maxFileSize <= 0 || maxFileSize > 50 {
		maxFileSize = 20
	}
	maxBackupCount := beego.AppConfig.DefaultInt("log_backup_count", 5)
	if maxBackupCount < 0 || maxBackupCount > 100 {
		maxBackupCount = 5
	}
	traceutils.RunLogRotate(&traceutils.LogRotateConfig{
		Dir:         filepath.Dir(logDir),
		MaxFileSize: maxFileSize,
		BackupCount: maxBackupCount,
		Period:      rotatePeriod,
	})
}
