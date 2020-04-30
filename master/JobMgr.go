package master

import (
	"context"
	"encoding/json"
	"github.com/MxAc/crontab/common"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

//通过etcd实现保存job
//整个模块通过与etcd交互来管理job

//任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
}


var(
	//单例
	G_jobMgr *JobMgr
)


//初始化管理器
func InitJobMgr()(err error){
	var(
		config clientv3.Config
		client  *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
	)


	//初始化配置
	config =clientv3.Config{
		Endpoints: G_config.EtcdEndpoints,  //集群地址
		DialTimeout:time.Duration(G_config.EtcdDialTimeout)*time.Millisecond,  //连接超时
	}

	//建立连接
	if client,err = clientv3.New(config);err!=nil{
		return
	}

	//得到kv和lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	//赋值单例
	G_jobMgr = &JobMgr{
		client:client,
		kv:kv,
		lease:lease,
	}
	return
}

//保存任务
func(jobMgr *JobMgr)SaveJob(job *common.Job)(oldJob *common.Job,err error){
	//把任务保存到/cron/jobs/任务名 -> json
	var(
		jobKey string
		jobValue []byte
		putResp *clientv3.PutResponse
		oldJobObj common.Job
	)

	//etcd的保存key
	jobKey = common.JOB_SAVE_DIR+job.Name
	//任务信息json
	if jobValue,err = json.Marshal(job);err!=nil{
		return
	}
	//保存到etcd  (字符串和二进制数组 的底层存储都是一个个的字节，因此类型转换无缝)
	if putResp,err=jobMgr.kv.Put(context.TODO(),jobKey,string(jobValue),clientv3.WithPrevKV());err!=nil{
		return
	}
	//如果是更新，那么返回旧值
	if putResp.PrevKv !=nil{
		//对旧值做一个反序列化
		if err=json.Unmarshal(putResp.PrevKv.Value,&oldJobObj);err!=nil{
			err=nil
			return
		}
		oldJob=&oldJobObj
	}
	return
}

//删除任务（etcd支持删除一个完全不存在的任务）
func(jobMgr *JobMgr)DeleteJob(name string)(oldJob *common.Job,err error){
	var(
		jobKey string
		delResp *clientv3.DeleteResponse
		oldJobObj common.Job
	)

	//etcd中保存任务的key
	jobKey = common.JOB_SAVE_DIR+name
	//删除key  WithPreKV() 返回被删除的内容
	if delResp,err=jobMgr.kv.Delete(context.TODO(),jobKey,clientv3.WithPrevKV());err!=nil{
		return
	}

	//返回被删除的任务信息
	if len(delResp.PrevKvs)!=0{
		//解析旧值到oldJobObj中，并返回   因为只删除了一个key，所以只有一个元素，即 PreKvs[0]
		if err = json.Unmarshal(delResp.PrevKvs[0].Value,&oldJobObj);err!=nil{
			err=nil
			return
		}
		//解析成功的话就赋值给返回值
		oldJob = &oldJobObj
	}

	return
}

//列举任务
func(jobMgr *JobMgr)ListJobs()(jobList []*common.Job,err error){
	var(
		dirKey string
		getResp *clientv3.GetResponse
		kvPair *mvccpb.KeyValue  //etcd中的一种结构
		job *common.Job
	)
	//任务保存的目录
	dirKey=common.JOB_SAVE_DIR
	//获取目录下所有的任务信息
	if getResp,err=jobMgr.kv.Get(context.TODO(),dirKey,clientv3.WithPrefix());err!=nil{
		return
	}

	//初始化数组空间
	jobList = make([]*common.Job,0)  //初始长度为0，元素为 *common.Job 的指针数组
	//数组内存空间不足时会重新分配一块内存把原来的元素拷贝进新的数组，内存地址会发生改变
	//len(jobList) == 0

	//遍历所有任务，进行反序列化
	for _,kvPair= range getResp.Kvs{
		job = &common.Job{}
		if err=json.Unmarshal(kvPair.Value,job);err!=nil{
			err=nil
			continue  //忽略反序列化中出现的错误，因为不太可能出现. 这里容忍个别job的反序列化失败
		}
		jobList = append(jobList, job)
	}
	return

}

//杀死任务

func (JobMgr *JobMgr) KillJob(name string)(err error){
	//更新key=/cron/killer/任务名，一旦更新，所有的worker会监听到k-v的变化，就会去杀死任务
	var(
		killerKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId clientv3.LeaseID
	)
	//通知worker杀死对应任务
	killerKey =common.JOB_KILLER_DIR+name

	//让worker监听到一次put操作。（设置一个过期时间，put完一次马上过期，用来节约etcd的存储空间）
	if leaseGrantResp,err=JobMgr.lease.Grant(context.TODO(),1);err!=nil{
		return
	}

	//租约id
	leaseId=leaseGrantResp.ID

	//设置killer标记
	if _,err=JobMgr.kv.Put(context.TODO(),killerKey,"",clientv3.WithLease(leaseId));err!=nil{
		return
	}
	return
}