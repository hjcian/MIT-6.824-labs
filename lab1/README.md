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
- `nReduce` 決定了 `map` 做完的任務，要切成幾個 reduce task；`map` task 得將做完的結果切成 `nReduce` 個 intermediate files


## How to run your codes
- 運行 master
- 運行多個 workers (e.g. 3 workers)

## 其他同學的答案參考
- https://mr-dai.github.io/mit-6824-lab1/
- https://zhuanlan.zhihu.com/p/260752052
- https://mr-dai.github.io/mapreduce_summary/