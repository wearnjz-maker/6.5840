package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Task struct {
	Source       string       // 来源
	TextID       int          // 文本编号
	SliceNum     int          // 任务总数
	Intermediate []KeyValue   // 文本内容
	Type         string       // 任务类型: "map" 或 "reduce"
	WorkerID     int          // 执行任务的worker编号
	Time         int64        // 任务开始时间
	SliceID      int          // 分片编号
	OFile        string       // 输出文件名
	ReduceKV     [][]KeyValue // reduce任务的中间结果
}

// Add your RPC definitions here.
