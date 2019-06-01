package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	var wg sync.WaitGroup
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
		taskChan := make(chan DoTaskArgs, ntasks)
		wg.Add(ntasks)
		for i := 0; i < ntasks; i++{
			args := DoTaskArgs{
				JobName: jobName,
				File: mapFiles[i],
				Phase: mapPhase,
				TaskNumber: i,
				NumOtherPhase: n_other}
				taskChan <- args
		}

		go func(){
			wg.Wait()
			close(taskChan)
		}()
		for args := range taskChan{
			//fmt.Println(args.TaskNumber, "map")
			arg := args
			go func(){
				workerAddr := <- registerChan
				if ok := call(workerAddr, "Worker.DoTask", arg, nil); !ok{
					taskChan <- arg
				}else{
					wg.Done()
				}
				registerChan <- workerAddr
			}()
		}

	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
		taskChan := make(chan DoTaskArgs, ntasks)
		wg.Add(ntasks)
		for i := 0; i < ntasks; i++{
			args := DoTaskArgs{
				JobName: jobName,
				File: mapFiles[i],
				Phase: reducePhase,
				TaskNumber: i,
				NumOtherPhase: n_other}	
			taskChan <- args
		}
		go func(){
			wg.Wait()
			close(taskChan)
		}()
		for args := range taskChan{
			arg := args
			go func(){
				workerAddr := <- registerChan
				if ok := call(workerAddr, "Worker.DoTask", arg, nil); !ok{
					taskChan <- arg
				}else{
					wg.Done()
				}
				registerChan <- workerAddr
			}()
		}
	}
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	fmt.Printf("Schedule: %v done\n", phase)
	wg.Wait()
}
