package config

import (
	"fmt"
	"github.com/LeonRhapsody/CdcClient/src/ogg"
	"github.com/LeonRhapsody/CdcClient/src/shell"
	"github.com/spf13/viper"
	"log"
	"time"
)

type RunConfig struct {
	Address         string        `yaml:"address"`
	Ssl             Ssl           `yaml:"ssl"`
	Port            string        `yaml:"port"`
	DownloadAddress string        `yaml:"notify_Address"`
	CheckInterval   time.Duration `yaml:"checkInterval""`
	Debug           bool          `yaml:"debug"`
}

type Ssl struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
	Ca   string `yaml:"ca"`
}

func defaultConfig() {

	viper.SetDefault("address", "0.0.0.0")
	viper.SetDefault("port", "34567")
	viper.SetDefault("cert", "./ogg.crt")
	viper.SetDefault("key", "./ogg.key")
	viper.SetDefault("ca", "./ca")
	viper.SetDefault("ORACLE_SID", "orcl")

}

func ReadConfig() RunConfig {
	defaultConfig()

	viper.SetConfigFile("conf/config.yaml")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %w \n", err))
	}
	var runConfig RunConfig
	viper.Unmarshal(&runConfig)
	return runConfig
}

func ReadEnv() ogg.RunEnv {

	viper.AutomaticEnv()

	viper.BindEnv("OGG_HOME")
	viper.BindEnv("ORACLE_SID")
	viper.BindEnv("USER")
	viper.BindEnv("ORACLE_HOME")

	if viper.Get("USER") != "oracle" {
		log.Printf("当前运行用户为 %s ,please change to [oracle],and try again\n", viper.Get("USER"))
		log.Println("如果当前是OGG For Bigdata，忽略上述错误")
	}
	if viper.Get("ORACLE_HOME") == nil {
		log.Println("env [ORACLE_HOME] not found,is oracle here?")
	}
	if viper.Get("OGG_HOME") == nil {
		viper.SetDefault("OGG_HOME", "/u01/app/ogg")
		log.Printf("env [OGG_HOME] not found,will use default path [/u01/app/ogg]")
	}

	defaultConfig()
	runEnv := ogg.RunEnv{
		USER:       fmt.Sprintf("%v", viper.Get("USER")),
		OracleHome: fmt.Sprintf("%v", viper.Get("ORACLE_HOME")),
		OggHome:    fmt.Sprintf("%v", viper.Get("OGG_HOME")),
		SID:        fmt.Sprintf("%v", viper.Get("ORACLE_SID")),
		IP:         shell.GetIP(),
	}

	return runEnv

}
