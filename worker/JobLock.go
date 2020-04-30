package worker

import (
	"context"
	"github.com/MxAc/crontab/common"
	"go.etcd.io/etcd/clientv3"
)

//分布式锁(通过执行一个txn事务去抢到一个key，抢占这个key即上锁)
type JobLock struct {
	//etcd客户端
	kv clientv3.KV
	lease clientv3.Lease  //通过lease让锁自动过期，避免节点宕机后锁永远被占用，宕机后锁会自动过期

	jobName string  //任务名 （不同的任务有不同的锁）
	cancelFunc context.CancelFunc //用于终止自动续租
	leaseId clientv3.LeaseID //租约ID
	isLocked bool  //是否上锁成功
}


//初始化一把锁
func InitJobLock(jobName string,kv clientv3.KV,lease clientv3.Lease)(jobLock *JobLock){
	jobLock = &JobLock{
		kv:      kv,
		lease:   lease,
		jobName: jobName,
	}
	return
}


//尝试上锁(抢乐观锁，抢不到拉倒)
func(jobLock *JobLock)TryLock()(err error){
	var(
		leaseGrantResp *clientv3.LeaseGrantResponse
		cancelCtx context.Context
		cancelFunc context.CancelFunc
		leaseId clientv3.LeaseID
		keepRespChan <-chan *clientv3.LeaseKeepAliveResponse
		txn clientv3.Txn
		lockKey string
		txnResp *clientv3.TxnResponse
	)
	//1.创建租约（5秒）
	if leaseGrantResp,err=jobLock.lease.Grant(context.TODO(),5);err!=nil{
		return
	}

	//context用于取消自动续租
	cancelCtx,cancelFunc = context.WithCancel(context.TODO())

	//租约ID
	leaseId = leaseGrantResp.ID

	//2.自动续租(上锁成功的话一直续租避免租约过期锁被释放)
	if keepRespChan,err=jobLock.lease.KeepAlive(cancelCtx,leaseId);err!=nil{
		goto FAIL   //续租失败即上锁失败
	}
	//3.处理续租应答的协程
	go func(){
		var(
			keepResp *clientv3.LeaseKeepAliveResponse
		)
		for{
			select{
			case keepResp=<-keepRespChan: //自动续租的应答
			if keepResp==nil{ //碰到空指针说明自动续租被取消掉了（可能是释放锁时也可能是异常时）
				goto END
			}

			}

		}
		END:
	}()


	//4.创建事务txn
	txn=jobLock.kv.Txn(context.TODO())
	//锁路径(抢到这个key就是抢到了锁)
	lockKey=common.JOB_LOCK_DIR + jobLock.jobName

	//4.事务抢锁(如果key 不存在的话就把key占一下，值传空值,这样节点宕机后锁也会被释放，其余的worker节点可以继续调度任务)
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey),"=",0)).
		Then(clientv3.OpPut(lockKey,"",clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockKey))
	//提交事务
	if txnResp,err=txn.Commit();err!=nil{
		goto FAIL  //失败的情况无法得知是否抢到锁事务是否写入etcd，只能去释放租约。确保任何异常状态下都可以回滚。
	}

	//5.成功返回，失败回滚，释放租约  (succeed意味着执行了txn 的then分支而不是else分支   )
	if !txnResp.Succeeded{  //锁被占用
		err=common.ERR_LOCK_ALREADY_REQUIRED
		goto FAIL
	}

	//抢锁成功，记录租约ID
	jobLock.leaseId = leaseId
	jobLock.cancelFunc=cancelFunc
	jobLock.isLocked=true
	return

	FAIL:
		cancelFunc()  //取消自动续租
		jobLock.lease.Revoke(context.TODO(),leaseId)  //释放租约,租约释放key马上过期
	    return
}

//释放锁
func(jobLock *JobLock)Unlock(){
	if jobLock.isLocked{
		jobLock.cancelFunc()  //取消程序自动续租的协程
		jobLock.lease.Revoke(context.TODO(),jobLock.leaseId)   //释放租约
	}

}
