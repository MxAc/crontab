package worker

import (
	"context"
	"github.com/MxAc/crontab/common"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

//通过etcd实现保存job
//整个模块通过与etcd交互来管理job,监听任务的变化

//任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	watcher clientv3.Watcher
}


var(
	//单例
	G_jobMgr *JobMgr
)


//监听任务变化

func(JobMgr *JobMgr)watchJobs()(err error){
	var(
		getResp *clientv3.GetResponse
		kvpair *mvccpb.KeyValue
		job *common.Job
		watchStartRevision int64
		waitChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobName string
		jobEvent *common.JobEvent
	)

	//1.get下 /cron/jobs/ 目录下的所有任务，并且得到当前集群的revision
	if getResp,err=JobMgr.kv.Get(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithPrefix());err!=nil{
		return
	}

	//当前有哪些任务
	for _,kvpair = range getResp.Kvs{
	//反序列化json得到Job
		if job,err=common.UnpackJob(kvpair.Value);err==nil{
			jobEvent=common.BuildJobEvent(common.JOB_EVENT_SAVE,job)
			//TODO:把job同步给scheduler（调度协程）
			G_scheduler.PushJobEvent(jobEvent)
		}
	}
	//2.从该revision向后监听变化事件
	go func(){   //监听协程
		watchStartRevision = getResp.Header.Revision + 1 //从get时的后续版本开始监听变化
		//启动监听/cron/jobs/目录的后续变化
		waitChan=JobMgr.watcher.Watch(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithRev(watchStartRevision),clientv3.WithPrefix())
		//处理监听事件 chan也是可以遍历的
		for watchResp = range  waitChan{
			for _,watchEvent=range watchResp.Events{
				switch watchEvent.Type {
				case mvccpb.PUT: //任务保存事件
				if job,err=common.UnpackJob(watchEvent.Kv.Value);err!=nil{
					continue
				}
				//构建一个更新Event
				jobEvent=common.BuildJobEvent(common.JOB_EVENT_SAVE,job)
				case mvccpb.DELETE: //任务被删除了
				// Delete  /cron/jobs/job10
				jobName=common.ExtractJobName(string(watchEvent.Kv.Key))
				job=&common.Job{Name:jobName}
				//构建一个删除Event
				jobEvent=common.BuildJobEvent(common.JOB_EVENT_DELETE,job)
				}
				//推送变化给scheduler
				G_scheduler.PushJobEvent(jobEvent)
			}
		}
	}()
	return
}


//监听强杀任务通知
func(jobMgr *JobMgr)watchKiller(){
	var(
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobEvent *common.JobEvent
		jobName string
		job *common.Job
	)

	//监听/cron/killer 目录
	//2.从该revision向后监听变化事件
	go func(){   //监听协程
		//启动监听/cron/killer/目录的后续变化
		watchChan=jobMgr.watcher.Watch(context.TODO(),common.JOB_KILLER_DIR,clientv3.WithPrefix())
		//处理监听事件 chan也是可以遍历的
		for watchResp = range  watchChan{
			for _,watchEvent=range watchResp.Events{
				switch watchEvent.Type {
				case mvccpb.PUT: //杀死任务事件
				jobName=common.ExtractKillerName(string(watchEvent.Kv.Key)) //  /cron/killer/job10
				job = &common.Job{Name:jobName}
				jobEvent=common.BuildJobEvent(common.JOB_EVENT_KILL,job)
				//事件推给scheduler
				G_scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE: //killer 标记过期，被自动删除
				}

			}
		}
	}()
}

//初始化管理器
func InitJobMgr()(err error){
	var(
		config clientv3.Config
		client  *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		watcher clientv3.Watcher
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
	watcher=clientv3.NewWatcher(client)


	//赋值单例
	G_jobMgr = &JobMgr{
		client:client,
		kv:kv,
		lease:lease,
		watcher:watcher,
	}


	//启动任务监听
	G_jobMgr.watchJobs()

	//启动监听killer
	G_jobMgr.watchKiller()

	return
}



//创建任务执行锁
func(jobMgr *JobMgr)CreateJobLock(jobName string)(jobLock *JobLock){
	//返回一把锁
	jobLock = InitJobLock(jobName,jobMgr.kv,jobMgr.lease)

    return
}