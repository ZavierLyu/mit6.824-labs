# MIT 6.824 (2022)

写Raft遇到的坑：

1. 在RPC调用后加减锁，不要调用前就加锁。否则会阻塞其他的handler和RPC调用
2. 注意livelocks:
   1. 重置election timer的时机确保有leader或者即将诞生leader：a)只在收到合法Leader的AppendEntries 和 b)回复拉票VoteGrant=true时 和c)自己开始新的选举后重置election timer。不然一直会有垃圾信息来打扰真正的candidate去选举。

## FAQ

1. AppendEntriesArgs为啥这里要传PrevLogIndex
   1. PrevLogIndex 的意思是：leader 要求 follower 先确认：“你的第 PrevLogIndex 条日志，term 是否等于 PrevLogTerm？”只有确认通过，follower 才能安全地接收后面的 Entries。
      例子：
      leader: [0] [1:T1] [2:T1] [3:T2] [4:T2]
      follower: [0] [1:T1] [2:T1] [3:T3] [4:T3] [5:T3]
      leader 想从 index 3 开始发送正确日志时，会发：
      PrevLogIndex = 2
      PrevLogTerm = T1
      Entries = [T2, T2]
      follower 检查自己的 index 2：
      index 2 的 term 是 T1，匹配
      于是它知道：前 0～2 条和 leader 一致；从 index 3 起可以删除旧的 T3 日志，并写入 leader 给的 T2 日志。
2. 为什么推进 commitIndex 需要满足两个条件：1.matchIndex 显示该日志已经复制到严格多数节点。2.该日志的 term 等于 Leader 当前的 currentTerm。
   第一个条件利用多数派交集，使已经提交的日志能够被未来的选举多数派观察到。按 matchIndex 排序后，取多数派对应的位置，就能得到当前多数节点共同复制到的最大日志 index。
   第二个条件用于保证 Leader Completeness。旧 term 的日志即使在某一时刻存在于多数节点，也不能仅凭副本数量直接提交，因为未来仍可能选出一个不包含该日志、index相等但最后日志 term 更高的 Leader，然后回退并覆盖这条旧日志。
   因此，Leader 只能通过多数派复制当前 term 的日志来直接推进 commitIndex。当当前 term 的日志提交后，它之前的全部日志会作为前序被间接提交。
   如果新 Leader 当选后一直没有客户端命令，那么旧日志可能一直无法通过这条规则被间接提交。所以新 Leader 当选后追加一条当前 term 的 no-op 日志；提交这条 no-op 后，之前的日志也会一起提交。

2022 Index Site: http://nil.csail.mit.edu/6.824/2022/
2022 Schedule: http://nil.csail.mit.edu/6.824/2022/schedule.html

## Labs

### [Lab 1](http://nil.csail.mit.edu/6.824/2022/labs/lab-mr.html)

### [Lab 2](http://nil.csail.mit.edu/6.824/2022/labs/lab-raft.html)
