package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
//import "sort"
import "strconv"


// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
    callMap(mapf)
}

func callMap(mapf func(string, string) []KeyValue) {
    args := MapArgs{}
    reply := MapReply{}
    call("Master.MapTask", &args, &reply)
	fmt.Printf("reply file %s\n", reply.Filename)

    file, err := os.Open(reply.Filename)
    if err != nil {
        log.Fatalf("cannot open %v\n", reply.Filename)
    }
    content, err := ioutil.ReadAll(file)
    if err != nil {
        log.Fatalf("cannot read %v", reply.Filename)
    }
    file.Close()
    kva := mapf(reply.Filename, string(content))

    //intermediate, err := os.OpenFile("mr-0-0", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    //if err != nil {
    //    log.Fatalf("cannot open %v", intermediate)
    //}

    //enc := json.NewEncoder(intermediate)
    mapNum := reply.Mtask
    nReduce := reply.Nreduce
    for _, kv := range kva {
        filename := "mr-" + strconv.Itoa(mapNum) + "-" + strconv.Itoa(ihash(kv.Key) % nReduce)

	    fmt.Printf("intermediate file %s\n", filename)

        intermediate, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
        if err != nil {
            log.Fatalf("cannot open intermediate file %v", intermediate)
        }

        enc := json.NewEncoder(intermediate)
        err = enc.Encode(&kv)
        if err != nil {
            log.Fatalf("cannot write intermediate file %v", intermediate)
        }
        intermediate.Close()
    }

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
