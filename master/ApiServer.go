package master

import (
	"encoding/json"
	"github.com/MxAc/crontab/common"
	"net"
	"net/http"
	"strconv"
	"time"
)

//任务的http接口
type ApiServer struct {
	httpServer *http.Server
}

//(每一个golang模块在全局来看其实都是一个单例)
//定义一个全局变量实现单例
var(
	//单例对象,想要被其它的包访问到则需要首字母大写
	G_apiServer *ApiServer
)


//保存任务接口,将任务保存到etcd中
//由客户端POST一个job字段  POST job={"name":"job1","command":"echo hello","cronExpr":"*****"}
func handleJobSave(resp http.ResponseWriter,req *http.Request) {
	var (
		err     error
		postJob string
		job     common.Job
		oldJob *common.Job
		bytes  []byte
	)
	//1.解析POST表单
	if err = req.ParseForm(); err != nil {
		goto ERR
	}
	//2.取表单中的job字段
	postJob = req.PostForm.Get("job")

	//3.反序列化job
	//postJob是字符串，直接通过这种方式强转成byte数组
	if err = json.Unmarshal([]byte(postJob), &job); err != nil {
		goto ERR
	}
	//4. 保存到etcd
	if oldJob,err=G_jobMgr.SaveJob(&job);err!=nil{
		goto  ERR
	}

	//5.返回正常应答  （{"errno":0,"msg":"","data":{...}}）
	if bytes,err = common.BuildResponse(0,"success",oldJob);err==nil{
		resp.Write(bytes)
	}
	return

	ERR:
    //6.返回异常应答
    if bytes,err=common.BuildResponse(-1,err.Error(),oldJob);err==nil{
    	resp.Write(bytes)
	}

}

//删除任务接口
// POST /job/delete  name=job1
func handleJobDelete(resp http.ResponseWriter,req *http.Request) {
	var(
		err error
		name string
		oldJob *common.Job
		bytes []byte
	)

	//POST: a=1 & b=2 & c=3
	if err = req.ParseForm();err!=nil{
		goto ERR
	}

	//删除的任务名
	name = req.PostForm.Get("name")

	//删除任务
	if oldJob,err= G_jobMgr.DeleteJob(name);err!=nil{
		goto ERR
	}

	//正常应答
	if bytes,err = common.BuildResponse(0,"success",oldJob);err==nil{
		resp.Write(bytes)
	}


	return

	ERR:
		//异常应答，返回异常信息
		if bytes,err = common.BuildResponse(-1,err.Error(),nil);err==nil{
			resp.Write(bytes)
		}



}


//列举所有crontab任务
func handleJobList(resp http.ResponseWriter,req *http.Request) {
	var(
		jobList []*common.Job
		bytes []byte
		err error
	)
	//获取任务列表
	if jobList,err = G_jobMgr.ListJobs();err!=nil{
		goto ERR
	}
	//返回正常应答
	if bytes,err = common.BuildResponse(0,"success",jobList);err==nil{
		resp.Write(bytes)
	}


	return

ERR:
	//异常应答，返回异常信息
	if bytes,err = common.BuildResponse(-1,err.Error(),nil);err==nil{
		resp.Write(bytes)
	}

}

//强制杀死某个任务
//POST /job/kill name=job1
func handleJobKill(resp http.ResponseWriter,req *http.Request){
	var(
		err error
		name string
		bytes []byte
	)
	//解析POST表单
	if err =req.ParseForm();err!=nil{
		goto ERR
	}

	name=req.PostForm.Get("name")
	//杀死任务
	if err=G_jobMgr.KillJob(name);err!=nil{
		goto ERR
	}

	//返回正常应答
	if bytes,err = common.BuildResponse(0,"success",nil);err==nil{
		resp.Write(bytes)
	}

	return

ERR:
	//异常应答，返回异常信息
	if bytes,err = common.BuildResponse(-1,err.Error(),nil);err==nil{
		resp.Write(bytes)
	}

}




//初始化服务（需要被其它包访问，首字母大写）
func InitApiServer()(err error){
	var(
		mux *http.ServeMux
		listener net.Listener
		httpServer *http.Server
		staticDir http.Dir  //静态文件根目录
		staticHandler http.Handler  //静态文件的http回调
	)
	//配置路由（代理模式）
	mux=http.NewServeMux()  //也是一个handler，不过作用是转发
	mux.HandleFunc("/job/save",handleJobSave) //内部根据请求的url遍历路由表，找到匹配的回调函数再进行流量转发
	mux.HandleFunc("/job/delete",handleJobDelete)
	mux.HandleFunc("/job/list",handleJobList)
	mux.HandleFunc("/job/kill",handleJobKill)

	//handler接口在这些动态路由中被重写。（handler的意义为处理方法）

	//静态文件目录(从磁盘中读取返回给浏览器)
	staticDir=http.Dir(G_config.WebRoot)
	//http中的FileServer返回值是一个handler，和上面的动态路由类似
	staticHandler=http.FileServer(staticDir)
	mux.Handle("/",http.StripPrefix("/",staticHandler))  //


	//启动TCP监听
	if listener,err=net.Listen("tcp",":"+ strconv.Itoa(G_config.ApiPort));err!=nil{
		return
	}


	//创建一个http服务。httpServer受到请求之后，会回调给Handler方法。
	httpServer=&http.Server{
		ReadTimeout:time.Duration(G_config.ApiReadTimeout) *time.Millisecond,
		WriteTimeout:time.Duration(G_config.ApiWriteTimeOut)*time.Millisecond,
		Handler:mux,  //路由。和handleJobSave是同类型回调函数，参数相同。
}

	//赋值单例
	G_apiServer = &ApiServer{httpServer:httpServer,}


	//启动了服务端
	go httpServer.Serve(listener)

	return
}