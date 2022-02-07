package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type CoordinatorStatus int
const (
	Idle CoordinatorStatus = iota
	InProgress
	Completed
)

type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

type Coordinator struct {
	// Your definitions here.
	TaskQueue     chan *Task          // 等待执行的task
	TaskMeta      map[int]*CoordinatorTask // 当前所有task的信息
	MasterPhase   State               // Master的阶段
	NReduce       int
	InputFiles    []string
	Intermediates [][]string // Map任务产生的R个中间文件的信息
}

type CoordinatorTask struct {
	TaskStatus    CoordinatorStatus
	StartTime     time.Time
	TaskReference *Task
}

type Task struct {
	Input         string
	TaskState     State
	NReducer      int
	TaskNumber    int
	Intermediates []string
	Output        string
}

// Your code here -- RPC handlers for the worker to call.
var mu sync.Mutex
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	ret := c.MasterPhase == Exit
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue:     make(chan *Task, max(nReduce, len(files))),
		TaskMeta:      make(map[int]*CoordinatorTask),
		MasterPhase:   Map,
		NReduce:       nReduce,
		InputFiles:    files,
		Intermediates: make([][]string, nReduce),
	}

	// 切成16MB-64MB的文件
	// 创建map任务
	c.createMapTask()

	// 一个程序成为master，其他成为worker
	//这里就是启动master 服务器就行了，
	//拥有master代码的就是master，别的发RPC过来的都是worker
	c.server()
	// 启动一个goroutine 检查超时的任务
	go c.catchTimeOut()
	return &c
}

func (c *Coordinator) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if c.MasterPhase == Exit {
			mu.Unlock()
			return
		}
		for _, masterTask := range c.TaskMeta {
			if masterTask.TaskStatus == InProgress && time.Now().Sub(masterTask.StartTime) > 10*time.Second {
				c.TaskQueue <- masterTask.TaskReference
				masterTask.TaskStatus = Idle
			}
		}
		mu.Unlock()
	}
}

func (c *Coordinator) createMapTask() {
	// 根据传入的filename，每个文件对应一个map task
	for idx, filename := range c.InputFiles {
		taskMeta := Task{
			Input:      filename,
			TaskState:  Map,
			NReducer:   c.NReduce,
			TaskNumber: idx,
		}
		c.TaskQueue <- &taskMeta
		c.TaskMeta[idx] = &CoordinatorTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}

func (c *Coordinator) createReduceTask() {
	c.TaskMeta = make(map[int]*CoordinatorTask)
	for idx, files := range c.Intermediates {
		taskMeta := Task{
			TaskState:     Reduce,
			NReducer:      c.NReduce,
			TaskNumber:    idx,
			Intermediates: files,
		}
		c.TaskQueue <- &taskMeta
		c.TaskMeta[idx] = &CoordinatorTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// master等待worker调用
func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	// assignTask就看看自己queue里面还有没有task
	mu.Lock()
	defer mu.Unlock()
	if len(c.TaskQueue) > 0 {
		//有就发出去
		*reply = *<-c.TaskQueue
		// 记录task的启动时间
		c.TaskMeta[reply.TaskNumber].TaskStatus = InProgress
		c.TaskMeta[reply.TaskNumber].StartTime = time.Now()
	} else if c.MasterPhase == Exit {
		*reply = Task{TaskState: Exit}
	} else {
		// 没有task就让worker 等待
		*reply = Task{TaskState: Wait}
	}
	return nil
}

func (c *Coordinator) TaskCompleted(task *Task, reply *ExampleReply) error {
	//更新task状态
	mu.Lock()
	defer mu.Unlock()
	if task.TaskState != c.MasterPhase || c.TaskMeta[task.TaskNumber].TaskStatus == Completed {
		// 因为worker写在同一个文件磁盘上，对于重复的结果要丢弃
		return nil
	}
	c.TaskMeta[task.TaskNumber].TaskStatus = Completed
	go c.processTaskResult(task)
	return nil
}

func (c *Coordinator) processTaskResult(task *Task)  {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Map:
		//收集intermediate信息
		for reduceTaskId, filePath := range task.Intermediates {
			c.Intermediates[reduceTaskId] = append(c.Intermediates[reduceTaskId], filePath)
		}
		if c.allTaskDone() {
			//获得所以map task后，进入reduce阶段
			c.createReduceTask()
			c.MasterPhase = Reduce
		}
	case Reduce:
		if c.allTaskDone() {
			//获得所以reduce task后，进入exit阶段
			c.MasterPhase = Exit
		}
	}
}

func (c *Coordinator) allTaskDone() bool {
	for _, task := range c.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}