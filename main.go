package main

import (
	"github.com/LeonRhapsody/CdcClient/src/config"
	"github.com/LeonRhapsody/CdcClient/src/ogg"
	"github.com/LeonRhapsody/overseer"
	"github.com/LeonRhapsody/overseer/fetcher"
	"github.com/gin-gonic/gin"
	"net/http"
)

type RunHttp struct {
	Address string
	Port    string
	Cert    string
	Key     string
}

func main() {

	runConfig := config.ReadConfig()
	runHttp := RunHttp{
		Address: runConfig.Address,
		Port:    runConfig.Port,
		Cert:    runConfig.Ssl.Cert,
		Key:     runConfig.Ssl.Key,
	}

	overseer.Run(overseer.Config{
		Program: runHttp.prog,
		Fetcher: &fetcher.HTTP{
			URL:      runConfig.DownloadAddress, //自动升级包位置
			Interval: runConfig.CheckInterval,   //版本检查间隔
		},
		Debug: runConfig.Debug, //控制日志模式
	})
}
func (r RunHttp) prog(state overseer.State) {

	runEnv := config.ReadEnv()

	gin.SetMode(gin.ReleaseMode) //全局设置环境

	route := gin.Default()

	oggGroup := route.Group("ogg")
	{
		oggGroup.GET("/status", func(c *gin.Context) {
			_, str := runEnv.GetOggInfo()
			c.String(http.StatusOK, str)
		})

		oggGroup.GET("/repair", func(c *gin.Context) {
			c.String(http.StatusOK, runEnv.RepairOgg())
		})

		oggGroup.GET("/getdef", func(c *gin.Context) {
			str := runEnv.GenDefgenConf()
			c.String(http.StatusOK, str)
		})

		oggGroup.GET("/reset/:name/:thread", func(c *gin.Context) {
			name := c.Param("name")
			thread := c.Param("thread")
			c.String(http.StatusOK, runEnv.ResetOgg(name, thread))
		})

		oggGroup.GET("/restart/:name", func(c *gin.Context) {
			name := c.Param("name")
			c.String(http.StatusOK, runEnv.RestartOgg(name))
		})

		oggGroup.GET("/UpdateRepDefFile/:from/:to", func(c *gin.Context) {
			from := c.Param("from")
			to := c.Param("to")
			str := ogg.UpdateRepDefFile(from, to)
			c.String(http.StatusOK, str)
		})

		oggGroup.GET("/head/:text", func(c *gin.Context) {
			text := c.Param("text")
			c.String(http.StatusOK, text)
		})

		oggGroup.GET("/version", func(c *gin.Context) {
			c.String(http.StatusOK, state.ID)
		})

	}

	address := r.Address + ":" + r.Port
	cert := r.Cert
	key := r.Key

	route.SetTrustedProxies([]string{"192.168.1.2"})
	err := route.RunTLS(address, cert, key)
	if err != nil {
		return
	}

}
