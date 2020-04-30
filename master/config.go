package master

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

//配置文件的解析类

//通过标签语法加载配置  (通过json的反序列化，从master.json中读取，并且解析到这些字段中)
type Config struct{
	ApiPort int   `json:"apiPort"`
	ApiReadTimeout int  `json:"apiReadTimeOut"`
	ApiWriteTimeOut int   `json:"apiWriteTimeOut"`
	EtcdEndpoints []string `json:"etcdEndpoints"`
	EtcdDialTimeout int `json:"etcdDialTimeout"`
	WebRoot string `json:"webroot"`
}

var(
	//单例
	G_config *Config
)

//加载配置
func InitConfig(filename string)(err error){
	var(
		content []byte
		conf Config
	)
	//1.把配置文件读进来
	if content,err=ioutil.ReadFile(filename);err!=nil{
		return
	}

	//2.做json反序列化，把内容反序列化到 Config对象的字段中，方便程序访问
	if err = json.Unmarshal(content,&conf);err!=nil{
		return
	}

	//3.赋值单例
	G_config = &conf


	fmt.Println(conf)

	return
}