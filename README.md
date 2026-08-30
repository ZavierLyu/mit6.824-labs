# MIT 6.824 (2022)
写Raft遇到的坑：
1. 在RPC调用后加减锁，不要调用前就加锁。否则会阻塞其他的handler和RPC调用
2. 注意livelocks:
   1. 重置election timer的时机确保有leader或者即将诞生leader：a)只在收到合法Leader的AppendEntries 和 b)回复拉票VoteGrant=true时 和c)自己开始新的选举后重置election timer。不然一直会有垃圾信息来打扰真正的candidate去选举。



2022 Index Site: http://nil.csail.mit.edu/6.824/2022/
2022 Schedule: http://nil.csail.mit.edu/6.824/2022/schedule.html

## Labs

### [Lab 1](http://nil.csail.mit.edu/6.824/2022/labs/lab-mr.html)

### [Lab 2](http://nil.csail.mit.edu/6.824/2022/labs/lab-raft.html)
