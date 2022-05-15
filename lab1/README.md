# lab1
```
6.824 - Spring 2020
6.824 Lab 1: MapReduce
```

- http://nil.lcs.mit.edu/6.824/2020/labs/lab-mr.html
- MapReduce 論文翻譯： https://www.cnblogs.com/fuzhe1989/p/3413457.html


## SPECs
- worker 與 master 使用 rpc 溝通
- Each worker process will ask the master for a task, read the task's input from one or more files, execute the task, and write the task's output to one or more files.
- The master should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same task to a different worker.

## How to run your codes
- 運行 master
- 運行多個 workers






