package mapreduce

import (
	"fmt"
	"sync"
	"sync/atomic"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	workerChan := make(chan string, ntasks)
	taskChan := make(chan int, ntasks)
	remain_tasks := int64(ntasks)
	var wg sync.WaitGroup

	for i := 0; i < ntasks; i++ {
		taskChan <- i
	}
	for remain_tasks > 0 {
		select {
		case newWorker := <-registerChan:
			// fmt.Printf("Schedule: adding worker %s to pool\n", newWorker)
			workerChan <- newWorker
		case worker := <-workerChan:
			wg.Add(1)
			go func(worker string, workerChan chan string, taskChan chan int) {
				defer wg.Done()
				// fmt.Printf("Schedule: scheduling work for worker %s\n", worker)
				task, ok := <-taskChan
				// All tasks have been performed
				if !ok {
					return
				}
				// Try to perform task
				args := new(DoTaskArgs)
				args.JobName = jobName
				args.File = mapFiles[task]
				args.Phase = phase
				args.TaskNumber = task
				args.NumOtherPhase = n_other
				// fmt.Printf("Schedule: Worker %s do task %d\n", worker, task)
				aok := call(worker, "Worker.DoTask", args, new(struct{}))
				// Worker is good to accept another task
				// workerChan <- worker
				if !aok {
					taskChan <- task
				} else {
					workerChan <- worker
					tasks_left := atomic.AddInt64(&remain_tasks, -1)
					if tasks_left == 0 {
						close(taskChan)
					}
				}
			}(worker, workerChan, taskChan)
		}
	}
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
