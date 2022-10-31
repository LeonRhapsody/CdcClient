package ogg

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"github.com/LeonRhapsody/CdcClient/src/shell"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"unsafe"
)

type ClientInfo struct {
	IP   string
	Port string
	Type string
}

type TaskInfo struct {
	Name string `json:"name"`
	IP   string `json:"ip"`
	Task []struct {
		SysID     string `json:"SysID"`
		Type      string `json:"Type"`
		Status    string `json:"Status"`
		Name      string `json:"Name"`
		CheckLag  string `json:"CheckLag"`
		CommitLag string `json:"CommitLag"`
		Error     string `json:"Error"`
	}
}

type RunEnv struct {
	ORACLE_HOME string `env:"ORACLE_HOME"`
	USER        string `env:"USER"`
	OGG_HOME    string `env:"OGG_HOME"`
	IP          string
}

func (r RunEnv) CmdOgg(cmd string) (string, error) {
	var bt bytes.Buffer
	bt.WriteString("( source ~/.bash_profile || source /etc/profile ) && echo \"")
	bt.WriteString(cmd)
	bt.WriteString("\"|")
	bt.WriteString(r.OGG_HOME)
	bt.WriteString("/ggsci")

	stdout, err := shell.Command(bt.String())
	return stdout, err

}

func (r RunEnv) GetOggInfo() (TaskInfo, string) {
	ip := r.IP
	result, err := r.CmdOgg("info all")
	if err != nil {
		log.Printf(result)
	}
	arr := strings.Split(result, "\n")

	var str2, strr string
	for _, val := range arr {
		arr := shell.Awk(val, " ")
		trace := ""
		if strings.HasPrefix(val, "MANAGER") {
			if arr[1] != "RUNNING" {
				trace = r.GetTrace(arr[0])
			}
			str := "{\"SysID\":\"\", \"Type\":\"MANAGER\", \"Status\":\"" + arr[1] + "\",\"Name\":\"MANAGER\",\"CheckLag\":\"\" ,\"CommitLag\":\"\",\"Error\":\"" + trace + "\"}"

			if str2 != "" {
				str2 = str2 + "," + str
			} else {
				str2 += str
			}
		}

		if len(arr) > 3 && strings.HasPrefix(arr[2], "EXT") {
			if arr[1] != "RUNNING" {
				trace = r.GetTrace(arr[2])
			}
			sysID := r.GetSysID(arr[2])
			str := "{\"SysID\":\"" + sysID + "\", \"Type\":\"EXTRACT\", \"Status\":\"" + arr[1] + "\", \"Name\":\"" + arr[2] + "\",\"CheckLag\":\"" + arr[3] + "\" ,\"CommitLag\":\"" + arr[4] + "\",\"Error\":\"" + trace + "\"}"
			if str2 != "" {
				str2 = str2 + "," + str
			} else {
				str2 += str
			}

		}
		if len(arr) > 3 && strings.HasPrefix(arr[2], "PUM") {
			if arr[1] != "RUNNING" {
				trace = r.GetTrace(arr[2])
			}
			sysID := r.GetSysID(arr[2])
			str := "{\"SysID\":\"" + sysID + "\", \"Type\":\"PUM\", \"Status\":\"" + arr[1] + "\", \"Name\":\"" + arr[2] + "\",\"CheckLag\":\"" + arr[3] + "\" ,\"CommitLag\":\"" + arr[4] + "\",\"Error\":\"" + trace + "\"}"
			if str2 != "" {
				str2 = str2 + "," + str
			} else {
				str2 += str
			}

		}

		if len(arr) > 3 && strings.HasPrefix(val, "REP") {
			if arr[1] != "RUNNING" {
				trace = r.GetTrace(arr[2])
			}
			sysID := r.GetSysID(arr[2])
			str := "{\"SysID\":\"" + sysID + "\", \"Type\":\"REP\", \"Status\":\"" + arr[1] + "\", \"Name\":\"" + arr[2] + "\",\"CheckLag\":\"" + arr[3] + "\" ,\"CommitLag\":\"" + arr[4] + "\",\"Error\":\"" + trace + "\"}"
			if str2 != "" {
				str2 = str2 + "," + str
			} else {
				str2 += str
			}

		}

	}
	strr = "{\"name\": \"ogg\",\"ip\": \"" + ip + "\",\"task\": [" + str2 + "]}"

	var t TaskInfo
	err = json.Unmarshal([]byte(strr), &t)

	if err != nil {

	}
	return t, strr

}

func (r RunEnv) RepairOgg() string {

	o, _ := r.GetOggInfo()
	str := ""
	if o.Task[0].Name == "MANAGER" && o.Task[0].Status != "RUNNING" {
		str += o.Task[0].Name + " "
		stdout, err := r.CmdOgg("start mgr")
		if err != nil {
			log.Printf(stdout)
		}

	}
	for _, val := range o.Task {

		if val.Status != "RUNNING" {
			stdout, err := r.CmdOgg("start " + val.Name)
			if err != nil {
				log.Printf(stdout)
			}
		}
	}

	return "repair task " + str

}

func (r RunEnv) ResetOgg(name string, thread string) string {
	if thread == "1" {
		stdout, err := r.CmdOgg("alter ext " + name + ",tranlog,thread 1,begin now ")
		if err != nil {
			log.Printf(stdout)
		}
	} else if thread == "2" {
		stdout, err := r.CmdOgg("alter ext " + name + ",tranlog,thread 1,begin now ")
		if err != nil {
			log.Printf(stdout)
		}
		stdout, err = r.CmdOgg("alter ext " + name + ",tranlog,thread 2,begin now ")
		if err != nil {
			log.Printf(stdout)
		}

	} else if thread == "3" {
		stdout, err := r.CmdOgg("alter ext " + name + ",tranlog,thread 1,begin now ")
		if err != nil {
			log.Printf(stdout)
		}
		stdout, err = r.CmdOgg("alter ext " + name + ",tranlog,thread 2,begin now ")
		if err != nil {
			log.Printf(stdout)
		}
		stdout, err = r.CmdOgg("alter ext " + name + ",tranlog,thread 3,begin now ")
		if err != nil {
			log.Printf(stdout)
		}
	} else if thread == "0" {
		stdout, err := r.CmdOgg("alter ext " + name + ",tranlog,begin now ")
		if err != nil {
			log.Printf(stdout)
		}
	}
	stdout, err := r.CmdOgg("start " + name)
	if err != nil {
		log.Printf(stdout)
	}

	return name + " reset success"

}

func (r RunEnv) GetTrace(name string) string {

	reportPath := r.OGG_HOME + "/dirrpt/" + name + ".rpt"
	isExits, _ := shell.PathExists(reportPath)
	if isExits {
		stdout, err := shell.Command("tail -10000 " + reportPath + "|grep ERROR|tr \"\\n\" \"$\"")
		if err != nil {
			log.Println(err)
		}
		return stdout
	}
	return "not found log"
}

func (r RunEnv) GenDefgenConf() string {

	o, _ := r.GetOggInfo()
	timeStamp := shell.GetTimestamp()
	dateStamp := shell.GetTimeForm(timeStamp)
	var (
		commonInfo string                                           //ogg用户 def格式信息
		tableInfo  string                                           //表信息
		configText string                                           //生成的defgen.prm 内容
		defText    string                                           //生成的表结构内容
		defName    = r.OGG_HOME + "/dirdef/common.def." + dateStamp //生成的def文件名
		filePath   = r.OGG_HOME + "/dirprm/common.prm"              //配置文件

	)

	for _, val := range o.Task {
		if strings.HasPrefix(val.Name, "EXT") {
			result, err := r.CmdOgg("view param " + val.Name)
			if err != nil {
				log.Printf(result)
			}
			arr := strings.Split(result, "\n")
			for _, val2 := range arr {

				if commonInfo == "" && strings.HasPrefix(val2, "USERID") { //获取commoninfo，只获取一次
					if strings.Contains(val2, "@") { //需要添加sid，否则会报错OGG-00664 ORA-12547 TNS:lost contact
						commonInfo = val2 + "\n" + "defsfile " + defName + ",format release 12.2\n"
					} else {
						tempString := ""
						for index, i := range shell.Awk(val2, ",") {
							if strings.Contains(i, "USERID") {
								i = i + "@orcl"
							}
							if index != 0 {
								i = " , " + i
							}
							tempString = tempString + i

						}
						commonInfo = tempString + "\n" + "defsfile " + defName + ",format release 12.2\n"
					}

				}

				if strings.HasPrefix(val2, "table ") { //获取表信息
					tableInfo = tableInfo + val2 + "\n"
				}
			}
		}
	}

	configText = commonInfo + tableInfo
	shell.Write(filePath, configText)

	cmd := r.OGG_HOME + "/defgen paramfile " + filePath
	line := "( source ~/.bash_profile || source /etc/profile ) && " + cmd

	stdout, err := shell.Command(line)
	if err != nil {
		log.Println(stdout, err)
	}

	defText, _ = shell.Read(defName)
	return defText

}

func (r RunEnv) GetSysID(extName string) string {
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
func (r RunEnv) RestartOgg(name string) string {

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

func UpdateRepDefFile(from string, repName string) string {

	c := ClientInfo{
		IP:   shell.Awk(from, ":")[0],
		Port: shell.Awk(from, ":")[1],
	}
	filename := "/u01/app/ogg/dirdef/" + repName + ".def"
	_, str := c.GetFromClient("getdef")
	shell.Write(filename, *str)
	return filename + " update success"
}
