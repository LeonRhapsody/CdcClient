package ogg

import (
	"crypto/tls"
	"encoding/json"
	"github.com/LeonRhapsody/CdcClient/src/shell"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"unsafe"
)

type ClientInfo struct {
	IP   string
	Port string
	Type string
}

type TaskInfo struct {
	Name  string `json:"name"`
	IP    string `json:"ip"`
	Tasks *[]Task
}

type RunEnv struct {
	OracleHome string `env:"ORACLE_HOME"`
	OggHome    string `env:"OGG_HOME"`
	USER       string `env:"USER"`
	IP         string
	SID        string `env:"ORACLE_SID"`
}

type Param struct {
	SysID         string
	SID           string
	Schema        string
	TaskName      string
	UserName      string
	Password      string
	TrailFileName string
	DefFileName   string
	IsADG         bool
	ISLogInAsm    bool
	TableList     []string
	RepIP         string
	RepPort       string
}

type Task struct {
	SysID     string `json:"SysID"`
	Type      string `json:"Type"`
	Status    string `json:"Status"`
	Name      string `json:"Name"`
	CheckLag  string `json:"CheckLag"`
	CommitLag string `json:"CommitLag"`
	Error     string `json:"Error"`
}

// CmdOgg 执行ogg命令，返回结果
func (r *RunEnv) CmdOgg(cmd string) (string, error) {

	bt := "( source ~/.bash_profile || source /etc/profile ) && echo " + cmd + "|" + r.OggHome + "/ggsci"

	stdout, err := shell.Command(bt)
	return stdout, err

}

func (r *RunEnv) getTaskInfoFromResult(context string) Task {
	var t0 Task
	arr := shell.Awk(context, " ")

	if strings.HasPrefix(context, "MANAGER") {
		t0.Name = "mgr"
		t0.Type = "MANAGER"
		t0.Status = arr[1]

		if t0.Status != "RUNNING" {
			t0.Error, _ = r.GetTrace(arr[0])
		}
		return t0
	}

	if strings.HasPrefix(context, "EXTRACT") || strings.HasPrefix(context, "REPLICAT") {
		t0.Name = arr[2]
		//EXT PUM REP
		t0.Type = shell.SubStr(t0.Name, 0, 3)
		t0.Status = arr[1]
		t0.SysID = r.GetSysID(t0.Name)
		t0.CheckLag = arr[3]
		t0.CommitLag = arr[4]

		t0.Error, _ = r.GetTrace(arr[2])
		return t0
	}

	return t0

}

// GetOggInfo 获取ogg进程的状态,返回struct 和 json两种
func (r *RunEnv) GetOggInfo() (*TaskInfo, string) {
	var t TaskInfo
	var ta []Task
	t.Name = "ogg"
	t.IP = r.IP

	result, err := r.CmdOgg("info all")
	if err != nil {
		log.Printf(result)
	}

	//逐行处理结果
	arr := strings.Split(result, "\n")

	for _, val := range arr {

		ta = append(ta, r.getTaskInfoFromResult(val))
	}
	t.Tasks = &ta

	statusJson, err := json.Marshal(&t)

	return &t, string(statusJson)

}

func (r *RunEnv) RepairOgg() string {

	o, _ := r.GetOggInfo()
	str := ""
	for _, task := range *(o.Tasks) {

		if task.Status != "RUNNING" {
			stdout, err := r.CmdOgg("start " + task.Name)
			if err != nil {
				log.Printf(stdout)
			}
			str = str + task.Name
		}
	}

	return "repair task " + str

}

func (r *RunEnv) ResetOgg(name string, thread string) string {

	threads, _ := strconv.Atoi(thread)

	if thread == "0" {
		stdout, err := r.CmdOgg("alter ext " + name + ",tranlog,begin now ")
		if err != nil {
			log.Printf(stdout)
		}
		return name + " reset success"
	}

	for i := 1; i < threads; i++ {
		stdout, err := r.CmdOgg("alter ext " + name + ",tranlog,thread " + strconv.Itoa(i) + ",begin now ")
		if err != nil {
			log.Printf(stdout)
		}
	}

	return name + " reset success"

}

func (r *RunEnv) GetTrace(name string) (string, error) {

	reportPath := r.OggHome + "/dirrpt/" + name + ".rpt"
	isExits, err := shell.PathExists(reportPath)
	if err != nil {
		return "", err
	}
	if isExits {
		stdout, err := shell.Command("tail -10000 " + reportPath + "|grep ERROR|tr \"\\n\" \"$\"")
		if err != nil {
			return "", err
		}
		return stdout, err
	}
	return "", err
}

// 读取配置文件获取ogg运行信息
// todo: 临时方案，未整合
func (r *RunEnv) getParam(sysID string) *Param {
	var p Param

	o, _ := r.GetOggInfo()
	for _, val := range *(o.Tasks) {
		//读取pum文件
		if val.SysID == sysID && val.Type == "PUM" {
			result, err := r.CmdOgg("view param " + val.Name)
			if err != nil {
				log.Printf(result)
			}
			//逐行切割
			arr := strings.Split(result, "\n")
			for _, line := range arr {

				if strings.HasPrefix(line, "USERID ") {

					info := shell.GetPramFromRegexp(line, `USERID (.*),(.*)PASSWORD (.*)`)

					if strings.Contains(info[1], `@`) { //可能存在SID
						p.UserName = shell.Awk(info[1], "@")[0]
						p.SID = shell.Awk(info[1], "@")[1]
					} else {
						p.UserName = info[1]
						p.SID = "ORCL"

					}

					p.Password = info[3]
				}

				if strings.HasPrefix(line, "TABLE ") {
					info := shell.GetPramFromRegexp(line, `TABLE (.*)`)
					p.Schema = shell.Awk(info[1], ".")[0]

				}

			}
		}

	}

	return &p

}

func (r *RunEnv) GenDefgenConf(sysID string) string {

	param := r.getParam(sysID)

	timeStamp := shell.GetTimestamp()
	dateStamp := shell.GetTimeForm(timeStamp)

	//最终生成的表结构文件
	param.DefFileName = r.OggHome + "/dirdef/" + sysID + ".def" + dateStamp

	//参数文件
	filePath := r.OggHome + "/dirprm/" + sysID + ".prm"

	//根据参数生成参数文件
	t := template.Must(template.ParseFiles("template/common.prm.tpl"))
	f, _ := os.Create(filePath)
	t.Execute(f, param)

	cmd := r.OggHome + "/defgen paramfile " + filePath
	line := "( source ~/.bash_profile || source /etc/profile ) && " + cmd

	//执行命令生产文件
	stdout, err := shell.Command(line)
	if err != nil {
		log.Println(stdout, err)
	}

	//明文读取
	defText, _ := shell.Read(param.DefFileName)
	return defText

}

func (r *RunEnv) GetSysID(extName string) string {
	sysID := "UNDEFINE"

	result, err := r.CmdOgg("view param " + extName)
	if err != nil {
		log.Printf(result)
	}
	arr := strings.Split(result, "\n")

	for _, val2 := range arr {
		if strings.Contains(val2, "SYSID") {
			list := strings.Split(val2, " ")
			sysID = list[len(list)-1]
		}
	}

	return sysID

}

func (r *RunEnv) RestartOgg(name string) string {

	stdout, err := r.CmdOgg("stop " + name)
	if err != nil {
		log.Printf(stdout)
	}
	stdout, err = r.CmdOgg("start " + name)
	if err != nil {
		log.Printf(stdout)
	}

	return "restart task " + name

}

func (c ClientInfo) GetFromClient(path string) ([]byte, *string) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr} //忽略证书检验

	uri := "https://" + c.IP + ":" + c.Port + "/ogg/" + path
	res, err := client.Get(uri)
	if err != nil {
		log.Println("Fatal error ", err.Error())
	}
	defer res.Body.Close()

	content, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Println("Fatal error ", err.Error())
	}

	str := (*string)(unsafe.Pointer(&content)) //转化为string,优化内存
	return content, str

}

func UpdateRepDefFile(from string, sysID string) string {

	c := ClientInfo{
		IP:   shell.Awk(from, ":")[0],
		Port: shell.Awk(from, ":")[1],
	}
	filename := "/u01/app/ogg/dirdef/" + sysID + ".def"
	_, str := c.GetFromClient("getdef/" + sysID)
	shell.Write(filename, *str)
	return filename + " update success"
}
