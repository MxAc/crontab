package worker

import (
	"fmt"
	"github.com/MxAc/crontab/common"
	"time"
)

//任务调度协程
type Scheduler struct {
	jobEventChan chan *common.JobEvent //使etcd任务事件队列
	jobPlanTable map[string]*common.JobSchedulePlan //任务调度计划表
	jobExcutingTable map[string]*common.JobExecuteInfo  //任务执行表
	jobResultChan chan *common.JobExecuteResult  //任务结果队列
}

var(
	G_scheduler *Scheduler
)


//处理任务事件
func (scheduler *Scheduler)handleJobEvent(jobEvent *common.JobEvent){
	var(
		jobSchedulePlan *common.JobSchedulePlan
		err error
		jobExisted bool
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
	)
	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE: //保存任务事件
	if jobSchedulePlan,err = common.BuildJobSchedulePlan(jobEvent.Job);err!=nil{
		return
	}
	scheduler.jobPlanTable[jobEvent.Job.Name]=jobSchedulePlan
	case common.JOB_EVENT_DELETE: //删除任务事件
	if jobSchedulePlan,jobExisted=scheduler.jobPlanTable[jobEvent.Job.Name];jobExisted{
		delete(scheduler.jobPlanTable,jobEvent.Job.Name)
	}
	case common.JOB_EVENT_KILL: //强杀任务事件
	//取消掉Command执行,判断任务是否在执行中
	if jobExecuteInfo,jobExecuting = scheduler.jobExcutingTable[jobEvent.Job.Name];jobExecuting{
		jobExecuteInfo.Cancelfunc()  //触发command杀死shell子进程，任务得到退出
	}

	}
}

//尝试执行任务
func(scheduler *Scheduler)TryStartJob(jobPlan *common.JobSchedulePlan){
	var(
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
	)
	//调度（定期检查哪些任务过期）和执行（发现过期任务之后再去执行）是两件事情

	//执行的任务可能运行很久。1分钟会调度60次只能执行一次,利用jobExecuteTable表实现去重防止并发。

	//如果任务正在执行，跳过本次调度
	if jobExecuteInfo,jobExecuting = scheduler.jobExcutingTable[jobPlan.Job.Name];jobExecuting{
		//fmt.Println("尚未退出，跳过执行",jobPlan.Job.Name)
		return
	}
	//构建执行状态信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)

	//保存执行状态
	scheduler.jobExcutingTable[jobPlan.Job.Name] = jobExecuteInfo

	//执行任务
	fmt.Println("执行任务：",jobExecuteInfo.Job.Name,jobExecuteInfo.PlanTime,jobExecuteInfo.RealTime)
	G_executor.ExecuteJob(jobExecuteInfo)


}


//重新计算任务调度状态
func (scheduler *Scheduler) TrySchedule()(scheduleAfter time.Duration){
	var(
		jobPlan *common.JobSchedulePlan
		now time.Time
		nearTime *time.Time
	)

	//如果任务表为空，随便睡多久
	if len(scheduler.jobPlanTable) == 0{
		scheduleAfter = 1*time.Second
		return
	}

	//当前时间
	now = time.Now()
	//1.遍历所有任务
	for _,jobPlan=range scheduler.jobPlanTable{
		if jobPlan.NextTime.Before(now)||jobPlan.NextTime.Equal(now){
			scheduler.TryStartJob(jobPlan)
			jobPlan.NextTime = jobPlan.Expr.Next(now) //更新下次执行时间
		}
		//统计最近一个要过期的任务时间
		if nearTime == nil||jobPlan.NextTime.Before(*nearTime){
			nearTime = &jobPlan.NextTime
		}
	}
	//下次调度间隔（最近要执行的任务调度时间 - 当前时间）
	scheduleAfter=(*nearTime).Sub(now)
	return



	//2.过期的任务立即执行



}

//处理任务结果
func(scheduler *Scheduler)handleJobResult(result *common.JobExecuteResult){
	var(
		jobLog *common.JobLog
	)

	//删除执行状态（从jobExcutingTable中删除正在执行的任务名字）
	delete(scheduler.jobExcutingTable,result.ExecuteInfo.Job.Name)

	//生成执行日志
	if result.Err !=common.ERR_LOCK_ALREADY_REQUIRED{  //如果不是锁被占用导致执行失败
		jobLog = &common.JobLog{
			JobName:      result.ExecuteInfo.Job.Name,
			Command:      result.ExecuteInfo.Job.Command,
			Err:          "",
			Output:       string(result.Output),
			PlanTime:     result.ExecuteInfo.PlanTime.UnixNano()/1000/1000,  //纳秒 微秒 毫秒
			ScheduleTime: result.ExecuteInfo.RealTime.UnixNano()/1000/1000,
			StartTime:    result.StartTime.UnixNano()/1000/1000,
			EndTime:      result.EndTime.UnixNano()/1000/1000,
		}
		if result.Err!=nil{
			jobLog.Err = result.Err.Error()
		}else{
			jobLog.Err=""
		}
	}
	//存储日志
	//（这一步不应该写在scheduleLoop（）里，不然容易使调度失去精度，甚至阻塞调度）
	G_logSink.Append(jobLog)


	fmt.Println("任务执行完成:",result.ExecuteInfo.Job.Name,result.Output,result.Err)
}


//调度协程
func(scheduler *Scheduler)scheduleLoop(){
	var(
		jobEvent *common.JobEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
		jobResult *common.JobExecuteResult
	)
	//初始化一次(1秒)
	scheduleAfter = scheduler.TrySchedule()

	//调度的延迟定时器
	scheduleTimer=time.NewTimer(scheduleAfter)

	//定时任务common.Job
	for{
		select{
		case jobEvent=<-scheduler.jobEventChan:  //监听任务变化事件
		//对内存维护的任务列表做增删改查
		scheduler.handleJobEvent(jobEvent)
		case  <-scheduleTimer.C: //最近的任务到期了
		case jobResult=<-scheduler.jobResultChan: //监听任务执行结果
		scheduler.handleJobResult(jobResult)
		//执行完shell之后，Executor会把结果返回给scheduler，scheduler通过channel监听就得到一个Result。
		}
		//调度一次任务
		scheduleAfter=scheduler.TrySchedule()
		//重置调度间隔(重新扫了一遍所有的任务所以知道了下次的任务调度的时间。可能有一次任务事件运行到这里，然后上次的任务还没有被触发，所以设置一下时间)
		scheduleTimer.Reset(scheduleAfter)

	}
}

//推送任务变化事件
func (scheduler *Scheduler)PushJobEvent(jobEvent *common.JobEvent){
	scheduler.jobEventChan <-jobEvent

}



//初始化调度器
func InitScheduler()(err error){
	G_scheduler = &Scheduler{
		jobEventChan:make(chan *common.JobEvent,1000),
		jobPlanTable:make(map[string]*common.JobSchedulePlan),
		jobExcutingTable:make(map[string]*common.JobExecuteInfo),
		jobResultChan: make(chan *common.JobExecuteResult),
	}
	//启动调度协程
	go G_scheduler.scheduleLoop()
	return
}

//回传任务执行结果
func (scheduler *Scheduler)PushJobResult(jobResult *common.JobExecuteResult){
	scheduler.jobResultChan<-jobResult
}
