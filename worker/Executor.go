package worker

import (
	"github.com/MxAc/crontab/common"
	"math/rand"
	"os/exec"
	"time"
)

//任务执行器
type Executor struct {

}


var (
	G_executor *Executor
)


//执行一个任务
//不加分布式锁的话，无法防止集群中任务并发调度，因为每一个worker节点都是在同一时刻调度同一任务，如果并发执行，任务就会被同时执行多次。
func(executor *Executor)ExecuteJob(info *common.JobExecuteInfo){
	go func(){
		var(
			cmd *exec.Cmd
			err error
			output []byte
			result *common.JobExecuteResult
			jobLock *JobLock
		)

		//任务结果
		result = &common.JobExecuteResult{
			ExecuteInfo:  info,
			Output:      make([]byte,0),
		}

		//首先获取分布式锁，如果锁到的话，再执行任务，没锁到再跳过。因为别的节点已经执行该任务，当前worker就不需要再执行。

		//初始化分布式锁
		jobLock=G_jobMgr.CreateJobLock(info.Job.Name)

		//记录任务开始时间
		result.StartTime = time.Now()

		//实际调度因为cpu是多核的，按照时间片分配任务，所以调度不是很精准，而不同的worker时间不是完全准确，可能会出现锁经常被一个节点抢占的情况。
		//这时候可以牺牲一点调度的准确性。时钟一般是微秒级别划分。
		//随机睡眠(0-1s) 只要不同节点的时间差别在  1 秒以内，经过随机的睡眠，worker的竞争机会就是随机的，就不会出现任务大部分被其中一个节点抢占的情况
		time.Sleep(time.Duration(rand.Intn(1000))*time.Millisecond)

		//上锁
		err = jobLock.TryLock()
		defer jobLock.Unlock()  //写在这里不管是if 还是 else 都不会忘记去释放锁

		if err!=nil{  //错误不为空上锁失败
			result.Err=err
			result.EndTime=time.Now()
		}else {
			//上锁成功后，重置任务启动时间
			result.StartTime = time.Now()

			//执行shell命令
			cmd=exec.CommandContext(info.CancelCtx,"/bin/bash","-c",info.Job.Command)
			//执行并捕获输出
			output,err=cmd.CombinedOutput()

			//记录任务结束时间
			result.EndTime=time.Now()
			result.Output=output
			result.Err=err

			//任务执行完，就把执行的结果返回给Scheduler，Scheduler会从executingTable中删除掉执行记录
			G_scheduler.PushJobResult(result)
		}
	}()
}


//初始化执行器
func InitExecutor()(err error){
	G_executor = &Executor{}
	return
}


