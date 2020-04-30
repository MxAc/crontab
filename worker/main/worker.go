package main

import (
	"flag"
	"fmt"
	"github.com/MxAc/crontab/worker"
	"runtime"
	"time"
)

var(
	confFile string //配置文件路径
)

//解析命令行参数
func initArgs(){
	//worker -config  ./worker.json
	//worker -h
	flag.StringVar(&confFile,"config","./worker.json","指定worker.json") //给用户的使用提示
	flag.Parse()   //解析所有的命令行参数
}




func initEnv(){
	runtime.GOMAXPROCS(runtime.NumCPU())
	//将最大线程数量初始化为CPU数量
}


func main(){
	var(
		err error
	)

	//初始化命令行参数
	initArgs()


	//初始化线程
	initEnv()

	//加载配置  (此处用到解析命令行参数的flag库)
	if err = worker.InitConfig(confFile);err!=nil{
		goto ERR
	}

	//启动日志协程
	if err = worker.InitLogSink();err!=nil{
		goto ERR
	}


	//启动执行器
	if err = worker.InitExecutor();err!=nil{
		goto ERR
	}

	//启动调度器
	if err = worker.InitScheduler();err!=nil{
		goto ERR
	}


	//初始化任务管理器
	if err = worker.InitJobMgr();err!=nil{
		goto ERR
	}


	//正常退出

	//使主函数不退出，所以其他协程可以运行完毕
	for{
		time.Sleep(1*time.Second)
	}


	return

	//异常退出
ERR:
	fmt.Println(err)


}