package worker

import (
	"context"
	"github.com/MxAc/crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

//mongodb存储日志
type LogSink struct{
	client *mongo.Client
	logCollection *mongo.Collection
	logChan chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}


var(
	//单例
	G_logSink *LogSink
)


//发送日志函数，批量写入日志
func(logSink *LogSink)saveLogs(batch *common.LogBatch){
	logSink.logCollection.InsertMany(context.TODO(),batch.Logs)
}

//日志存储协程（是一个写循环）
func(logSink *LogSink)writeLoop(){
	var(
		log *common.JobLog
		logBatch *common.LogBatch //当前的批次
		commitTimer *time.Timer
		timeOutBatch *common.LogBatch //超时批次
	)
	for{
		select{
		case log=<-logSink.logChan:
			//把这条log写到mongodb中
			//logSink.logCollecton.InsertOne
			//每次插入需要等待一次mongodb的请求往返，耗时可能因为网络慢而化为比较长的时间.所以先不急着插入，攒一下批次
			if logBatch==nil{  //此时 当前log为第一行日志，logBatch一开始是一条空指针
				logBatch = &common.LogBatch{}
				//让这个批次超时自动提交（给1秒的时间）
				commitTimer = time.AfterFunc(time.Duration(G_config.JobLogCommitTimeout)*time.Millisecond,
					func(batch *common.LogBatch) func(){
					//发出超时通知，不要直接提交batch。这里writeloop和回调函数是两个协程，对同一个batch操作就是并发操作，很麻烦
						//logSink.autoCommitChan <-logBatch  这种写法不对，因为logBatch 的值很可能会在别的协程修改掉
						return func(){
							logSink.autoCommitChan<- batch
							//这种做法传入的batch在闭包的上下文中，与函数外面的logBatch 变量无关
						}
					}(logBatch),
					)
			}

			//把新日志追加到批次中去
			logBatch.Logs = append(logBatch.Logs,log)

			//如果批次满了，就立即发送
			if len(logBatch.Logs) >= G_config.JobLogBatchSize{
				//发送日志
				logSink.saveLogs(logBatch)
				//清空logBatch
				logBatch = nil
				//取消定时器
				commitTimer.Stop()
				//可能这里定时器timer已经被触发，刚好在1秒的时间点批次满了，上面的闭包中的logSink.autoCommitChan 也被投递。
				//此时还会进入下面的分支，但是再次提交就会有bug，因此下面的分支需要先进行判断
			}
			case timeOutBatch = <-logSink.autoCommitChan: //过期的批次
			//判断过期的批次是否仍旧是当前的批次
			if timeOutBatch!=logBatch{  //说明logBatch已经被清空甚至进入了下一个Batch
				continue  //跳过已经被提交的批次
			}
			 //把批次写入到mongodb中去
			 logSink.saveLogs(timeOutBatch)
			 //清空logBatch
			 logBatch = nil
		}
	}
}

func InitLogSink()(err error){
	var(
		client *mongo.Client
		ctx context.Context
	)
	//建立mongodb连接
	ctx,_=context.WithTimeout(context.Background(), time.Duration(G_config.MongodbConnectTimeout) * time.Millisecond)
	if client,err=mongo.Connect(ctx,options.Client().ApplyURI(G_config.MongodbUri));err!=nil{
		return
	}

	//选择db和collection
	G_logSink=&LogSink{
		client: client,
		logCollection: client.Database("cron").Collection("log"),
		logChan: make(chan *common.JobLog,1000),
		autoCommitChan:make(chan *common.LogBatch,1000),
	}

	//启动一个mongodb处理协程
	go G_logSink.writeLoop()
	return
}

//发送日志
func(logSink *LogSink) Append(joblog *common.JobLog){
	select {
		case logSink.logChan <-joblog:  //日志没满的情况
		default:
			//直接返回。 队列满了就丢弃，不存了
	}
	//logSink.logChan <-joblog  只有一个写入线程，直接这么写channel可能会满
}