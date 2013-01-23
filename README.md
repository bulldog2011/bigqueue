bigqueue
========

A big, fast and persistent queue implemented in java.

feature highlights:  
1. **Fast**: close to the speed of direct memory access, both enqueue and dequeue are close to O(1) memory access.  
2. **Big**: the total size of the queue is only limited by the available disk space.  
3. **Persistent**: all data in the queue is persisted on disk, and is crash resitant.  
4. **Memory-efficient**: automatic paging & swapping algorithm, only most-recently accessed data is kept in memory.  
5. **Thread-safe**: multiple threads can concurrently enqueue and dequeue without data corruption.  
6. **Simple&Light-weight**: current number of source files is 12 and the library jar is less than 20K.

##Docs
[here is a simple design doc](http://bulldog2011.github.com/blog/2013/01/23/big-queue-design/)