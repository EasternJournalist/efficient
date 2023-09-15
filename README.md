# efficient

Utilities for efficient python

## Decorators

* `rate_limit`: limit the rate of a function / coroutine
* `concurrent_limit`: limit the concurrency of a function / coroutine
* `retry`: retry a function multiple times
* `safe`: catch exceptions and returning a default value
* `disk_cache`: cache a function's return value to disk files
* `worker_on_child_process`: run a function on a child process
* `workers_on_child_processes` run a function on multiple child processes
* `timeit`: time a function / coroutine
* `fake_serializable`: make a class always serializable by reinitializing from init arguments

## Classes

* `DynamicBatcher`: submit tasks one by one asynchroneously, execute them in batches automatically
* `WorkerOnChildProcess`: run a function on a child process
* `WorkersOnChildProcesses`: run a function on multiple child processes
* `Scheduler`: LRU scheduler for asynchronous tasks

## Scenarios

* Batch query ChatGPT with rate limit, concurrent limit and retry and safe...

* Use `workers_on_child_processes` to build a service, with multiple GPUs to host multiple models, robust to single model failure

* Use `DynamicBatcher` to build a HTTP service, automatically batchify requests for better performance