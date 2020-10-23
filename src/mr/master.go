package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Task struct {
    Filename string
    Assigned bool
}

type SyncMap struct {
    sync.Mutex
	Map []Task
}

type Master struct {
	// Your definitions here.
    // N*M buckets
	Nreduce int
	Mtask SyncMap
}


// Your code here -- RPC handlers for the worker to call.

func (m *Master) MapTask(args *MapArgs, reply *MapReply) error {
    m.Mtask.Lock()
    defer m.Mtask.Unlock()
    for i, task := range m.Mtask.Map {
        if !task.Assigned {
            task.Assigned = true
            reply.Filename = task.Filename
            reply.Nreduce = m.Nreduce
            reply.Mtask = i
            break
        }
    }
    return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.Nreduce = nReduce
    m.Mtask = SyncMap{Map: make([]Task, len(files)),}
	for i, filename := range files {
		m.Mtask.Map[i].Filename = filename
	}

	m.server()
	return &m
}
