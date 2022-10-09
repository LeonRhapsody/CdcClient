package shell

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"
)

func Command(cmd string) (string, error) {
	c := exec.Command("bash", "-c", cmd)
	// 此处是windows版本
	// c := exec.Command("cmd", "/C", cmd)
	output, err := c.CombinedOutput()
	return string(output), err
}

func Read(fileName string) (string, string) {
	f, err := os.Open(fileName)
	if err != nil {
		fmt.Println("read file fail", err)
		return "", ""
	}
	md5, _ := Md5SmallFile(fileName)
	defer f.Close()

	fd, err := ioutil.ReadAll(f)
	if err != nil {
		fmt.Println("read to fd fail", err)
		return "", ""
	}

	return string(fd), md5
}

func Write(fileName string, string2 string) {
	f, err3 := os.Create(fileName) //创建文件
	if err3 != nil {
		fmt.Println("create file fail")
	}
	w := bufio.NewWriter(f) //创建新的 Writer 对象
	n4, err3 := w.WriteString(string2)
	fmt.Printf("写入 %d 个字节\n", n4)
	err := w.Flush()
	if err != nil {
		return
	}
	err = f.Close()
	if err != nil {
		return
	}
}

func Md5SmallFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	h := md5.New()
	_, err = io.Copy(h, file)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

func Md5BigFile(path string) (string, error) {
	var fileChunk uint64 = 10485760
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	// calculate the file size
	info, _ := file.Stat()
	fileSize := info.Size()
	blocks := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))
	h := md5.New()

	for i := uint64(0); i < blocks; i++ {
		blockSize := int(math.Min(float64(fileChunk), float64(fileSize-int64(i*fileChunk))))
		buf := make([]byte, blockSize)

		_, err := file.Read(buf)
		if err != nil {
			return "", err
		}
		_, err = io.WriteString(h, string(buf)) // append into the hash
		if err != nil {
			return "", err
		}
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

func MD5String(v string) string {
	d := []byte(v)
	m := md5.New()
	m.Write(d)
	return hex.EncodeToString(m.Sum(nil))
}

func ReadLine(fileName string) ([]string, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	buf := bufio.NewReader(f)
	var result []string
	for {
		line, err := buf.ReadString('\n')
		line = strings.TrimSpace(line)
		if err != nil {
			if err == io.EOF { //读取结束，会报EOF
				return result, nil
			}
			return nil, err
		}
		result = append(result, line)
	}
	return result, nil
}

func GetTimestamp() int64 {
	timestamp := time.Now().Unix()
	return timestamp
}

func GetTimeForm(strTime int64) string {
	timeLayout := "2006-01-02—15:04:05"
	datetime := time.Unix(strTime, 0).Format(timeLayout)
	return datetime
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	//isnotexist来判断，是不是不存在的错误
	if os.IsNotExist(err) { //如果返回的错误类型使用os.isNotExist()判断为true，说明文件或者文件夹不存在
		return false, nil
	}
	return false, err //如果有错误了，但是不是不存在的错误，所以把这个错误原封不动的返回
}

func Awk(str string, sep string) []string { //awk '{print $N}'
	arr := strings.Split(str, sep)
	var result []string
	for _, val := range arr {

		if val != "" {
			result = append(result, val)

		}
	}
	return result

	//从切片 a 中删除索引为 index 的元素，操作方法是 a = append(a[:index], a[index+1:]...)
}

func GetIP() string {
	var ip string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println(err)
	}
	for _, address := range addrs {
		// 检查ip地址判断是否回环地址
		if ipNet, ok := address.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				if ip == "" {
					ip = ipNet.IP.String()
				} else {
					ip = ip + "/" + ipNet.IP.String()
				}

			}
		}

	}
	return ip

}
