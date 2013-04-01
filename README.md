# Big Queue


A big, fast and persistent queue based on memory mapped file.

##Feature Highlight:  
1. **Fast**: close to the speed of direct memory access, both enqueue and dequeue are close to O(1) memory access.  
2. **Big**: the total size of the queue is only limited by the available disk space.  
3. **Persistent**: all data in the queue is persisted on disk, and is crash resistant.
4. **Reliable**: OS will be responsible to presist the produced messages even your process crashes.  
5. **Realtime**: messages produced by producer threads will be immediately visible to consumer threads.
6. **Memory-efficient**: automatic paging & swapping algorithm, only most-recently accessed data is kept in memory.  
7. **Thread-safe**: multiple threads can concurrently enqueue and dequeue without data corruption.  
8. **Simple&Light-weight**: current number of source files is 12 and the library jar is less than 30K.


## The Big Picture

###Memory Mapped Sliding Window

![design](http://bulldog2011.github.com/images/luxun/sliding_window.png)


##Performance Highlight:
* In concurrent producing and consuming case, the average throughput is around ***166M bytes per second***.
* In sequential producing then consuming case, the average throughput is around ***333M bytes per second***.

Suppose the average message size is 1KB, then big queue can concurrently producing and consuming  
166K message per second. Basically, the throughput is only limited by disk IO bandwidth.

[here is a detailed performance report](https://github.com/bulldog2011/bigqueue/wiki/Performance-Test-Report)

##How to Use
1. Direct jar or source reference  
Download jar from repository mentioned in version history section below, latest stable release is [0.7.0](https://github.com/bulldog2011/bulldog-repo/tree/master/repo/releases/com/leansoft/bigqueue/0.7.0).   
***Note*** : bigqueue depends on log4j, please also added log4j jar reference if you use bigqueue.

2. Maven reference  

		<dependency>
		  <groupId>com.leansoft</groupId>
		  <artifactId>bigqueue</artifactId>
          <version>0.7.0</version>
		</dependency>
		
		<repository>
		  <id>github.release.repo</id>
		  <url>https://raw.github.com/bulldog2011/bulldog-repo/master/repo/releases/</url>
		</repository>


##Docs

1. [a simple design doc](http://bulldog2011.github.com/blog/2013/01/23/big-queue-design/)
2. [big queue tutorial](http://bulldog2011.github.com/blog/2013/01/24/big-queue-tutorial/)
3. [fanout queue tutorial](http://bulldog2011.github.com/blog/2013/03/25/fanout-queue-tutorial/)
4. [big array tutorial](http://bulldog2011.github.com/blog/2013/01/24/big-array-tutorial/)
5. [how to turn big queue into a thrift based queue service](http://bulldog2011.github.com/blog/2013/01/27/thrift-queue/)
6. [use case : producing and consuming 4TB log daily on one commodity machine](http://bulldog2011.github.com/blog/2013/01/28/log-collecting/)
7. [use case : sort and search 100GB data on a single commodity machine](http://bulldog2011.github.com/blog/2013/01/25/merge-sort-using-big-queue/)
8. [the architecture and design of a pub-sub messaging system tailored for big data collecting and analytics](http://bulldog2011.github.com/blog/2013/03/27/the-architecture-and-design-of-a-pub-sub-messaging-system/)

## Version History

#### 0.7.0 - *March 24, 2013* : [repository](https://github.com/bulldog2011/bulldog-repo/tree/master/repo/releases/com/leansoft/bigqueue/0.7.0)
  * Feature: support fanout queue semantics
  * Enhancement: make data file size configurable

#### 0.6.1 — *January 29, 2013* : [repository](https://github.com/bulldog2011/bulldog-repo/tree/master/repo/releases/com/leansoft/bigqueue/0.6.1)

  * Initial version:)


##Copyright and License
Copyright 2012 Leansoft Technology <51startup@sina.com>

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this work except in compliance with the License. You may obtain a copy of the License in the LICENSE file, or at:

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

