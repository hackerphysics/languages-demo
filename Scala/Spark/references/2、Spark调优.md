# Spark调优



 [spark经过优化后的HashShuffle过程.pptx](1、sparkSQL第二次课.assets\spark经过优化后的HashShuffle过程.pptx) 

# 1. 分配更多的资源

~~~
分配更多的资源：
	它是性能优化调优的王道，就是增加和分配更多的资源，这对于性能和速度上的提升是显而易见的，
	基本上，在一定范围之内，增加资源与性能的提升，是成正比的；写完了一个复杂的spark作业之后，进行性能调优的时候，首先第一步，就是要来调节最优的资源配置；
	在这个基础之上，如果说你的spark作业，能够分配的资源达到了你的能力范围的顶端之后，无法再分配更多的资源了，公司资源有限；那么才是考虑去做后面的这些性能调优的点。

相关问题：
（1）分配哪些资源？
（2）在哪里可以设置这些资源？
（3）剖析为什么分配这些资源之后，性能可以得到提升？
~~~

## 1.1 分配哪些资源

~~~
executor-memory、executor-cores、driver-memory
~~~

1.2 在哪里可以设置这些资源

~~~shell
在实际的生产环境中，提交spark任务时，使用spark-submit shell脚本，在里面调整对应的参数。
 	
 提交任务的脚本:
 spark-submit \
 --master spark://node1:7077 \
 --class com.kaikeba.WordCount \
 --num-executors 3 \    配置executor的数量
 --driver-memory 1g \   配置driver的内存（影响不大）
 --executor-memory 1g \ 配置每一个executor的内存大小
 --executor-cores 3 \   配置每一个executor的cpu个数
 /export/servers/wordcount.jar
 
~~~



## 1.2 参数调节到多大，算是最大

* ==Standalone模式==

~~~
 	先计算出公司spark集群上的所有资源 每台节点的内存大小和cpu核数，
 	比如：一共有20台worker节点，每台节点8g内存，10个cpu。
 	实际任务在给定资源的时候，可以给20个executor、每个executor的内存8g、每个executor的使用的cpu个数10。
~~~

* ==Yarn模式==

~~~
 	先计算出yarn集群的所有大小，比如一共500g内存，100个cpu；
 	这个时候可以分配的最大资源，比如给定50个executor、每个executor的内存大小10g,每个executor使用的cpu个数为2。
~~~

* 使用原则

~~~
在资源比较充足的情况下，尽可能的使用更多的计算资源，尽量去调节到最大的大小
~~~

## 1.3 为什么调大资源以后性能可以提升

![spark性能优化--分配资源](Spark调优.assets/spark性能优化--分配资源.png)

~~~shell
--executor-memory

--total-executor-cores 
~~~



# 2. 提高并行度

## 2.1 Spark的并行度指的是什么

~~~
 spark作业中，各个stage的task的数量，也就代表了spark作业在各个阶段stage的并行度！
    当分配完所能分配的最大资源了，然后对应资源去调节程序的并行度，如果并行度没有与资源相匹配，那么导致你分配下去的资源都浪费掉了。同时并行运行，还可以让每个task要处理的数量变少（很简单的原理。合理设置并行度，可以充分利用集群资源，减少每个task处理数据量，而增加性能加快运行速度。）
~~~

## 2.2 如何提高并行度

### 2.2.1 可以设置task的数量

~~~
至少设置成与spark Application 的总cpu core 数量相同。
最理想情况，150个core，分配150task，一起运行，差不多同一时间运行完毕
官方推荐，task数量，设置成spark Application 总cpu core数量的2~3倍 。
	
比如150个cpu core ，基本设置task数量为300~500. 与理想情况不同的，有些task会运行快一点，比如50s就完了，有些task 可能会慢一点，要一分半才运行完，所以如果你的task数量，刚好设置的跟cpu core 数量相同，可能会导致资源的浪费。
	因为比如150个task中10个先运行完了，剩余140个还在运行，但是这个时候，就有10个cpu core空闲出来了，导致浪费。如果设置2~3倍，那么一个task运行完以后，另外一个task马上补上来，尽量让cpu core不要空闲。同时尽量提升spark运行效率和速度。提升性能。
~~~



### 2.2.2 如何设置task数量来提高并行度

~~~
设置参数spark.default.parallelism
   默认是没有值的，如果设置了值为10，它会在shuffle的过程才会起作用。
   比如 val rdd2 = rdd1.reduceByKey(_+_) 
   此时rdd2的分区数就是10
   
可以通过在构建SparkConf对象的时候设置，例如：
   new SparkConf().set("spark.defalut.parallelism","500")
~~~



### 2.2.3 给RDD重新设置partition的数量

~~~
使用rdd.repartition 来重新分区，该方法会生成一个新的rdd，使其分区数变大。
此时由于一个partition对应一个task，那么对应的task个数越多，通过这种方式也可以提高并行度。
~~~



### 2.2.4 提高sparksql运行的task数量

http://spark.apache.org/docs/2.3.3/sql-programming-guide.html

~~~
通过设置参数 spark.sql.shuffle.partitions=500  默认为200；
可以适当增大，来提高并行度。 比如设置为 spark.sql.shuffle.partitions=500
~~~

专门针对sparkSQL来设置的



# 3. RDD的重用和持久化

## 3.1 实际开发遇到的情况说明

![rdd重用1](Spark调优.assets/rdd重用1.png)

~~~
如上图所示的计算逻辑：
（1）当第一次使用rdd2做相应的算子操作得到rdd3的时候，就会从rdd1开始计算，先读取HDFS上的文件，然后对rdd1做对应的算子操作得到rdd2,再由rdd2计算之后得到rdd3。同样为了计算得到rdd4，前面的逻辑会被重新计算。

（3）默认情况下多次对一个rdd执行算子操作，去获取不同的rdd，都会对这个rdd及之前的父rdd全部重新计算一次。
这种情况在实际开发代码的时候会经常遇到，但是我们一定要避免一个rdd重复计算多次，否则会导致性能急剧降低。

总结：可以把多次使用到的rdd，也就是公共rdd进行持久化，避免后续需要，再次重新计算，提升效率。
~~~

![rdd重用2](Spark调优.assets/rdd重用2.png)



## 3.2 如何对rdd进行持久化

* 可以调用rdd的cache或者persist方法。

~~~
（1）cache方法默认是把数据持久化到内存中 ，例如：rdd.cache ，其本质还是调用了persist方法
（2）persist方法中有丰富的缓存级别，这些缓存级别都定义在StorageLevel这个object中，可以结合实际的应用场景合理的设置缓存级别。例如： rdd.persist(StorageLevel.MEMORY_ONLY),这是cache方法的实现。
~~~

## 3.3 rdd持久化的时可以采用序列化

~~~
（1）如果正常将数据持久化在内存中，那么可能会导致内存的占用过大，这样的话，也许会导致OOM内存溢出。
（2）当纯内存无法支撑公共RDD数据完全存放的时候，就优先考虑使用序列化的方式在纯内存中存储。将RDD的每个partition的数据，序列化成一个字节数组；序列化后，大大减少内存的空间占用。
（3）序列化的方式，唯一的缺点就是，在获取数据的时候，需要反序列化。但是可以减少占用的空间和便于网络传输
（4）如果序列化纯内存方式，还是导致OOM，内存溢出；就只能考虑磁盘的方式，内存+磁盘的普通方式（无序列化）。
（5）为了数据的高可靠性，而且内存充足，可以使用双副本机制，进行持久化
	持久化的双副本机制，持久化后的一个副本，因为机器宕机了，副本丢了，就还是得重新计算一次；
	持久化的每个数据单元，存储一份副本，放在其他节点上面，从而进行容错；
	一个副本丢了，不用重新计算，还可以使用另外一份副本。这种方式，仅仅针对你的内存资源极度充足。
	 比如: StorageLevel.MEMORY_ONLY_2
~~~



# 4.  广播变量的使用

## 4.1 场景描述

~~~
	在实际工作中可能会遇到这样的情况，由于要处理的数据量非常大，这个时候可能会在一个stage中出现大量的task，比如有1000个task，这些task都需要一份相同的数据来处理业务，这份数据的大小为100M，该数据会拷贝1000份副本，通过网络传输到各个task中去，给task使用。这里会涉及大量的网络传输开销，同时至少需要的内存为1000*100M=100G，这个内存开销是非常大的。不必要的内存的消耗和占用，就导致了你在进行RDD持久化到内存，也许就没法完全在内存中放下；就只能写入磁盘，最后导致后续的操作在磁盘IO上消耗性能；这对于spark任务处理来说就是一场灾难。

    由于内存开销比较大，task在创建对象的时候，可能会出现堆内存放不下所有对象，就会导致频繁的垃圾回收器的回收GC。GC的时候一定是会导致工作线程停止，也就是导致Spark暂停工作那么一点时间。频繁GC的话，对Spark作业的运行的速度会有相当可观的影响。

~~~

![task共享数据](Spark调优.assets/task共享数据.png)



## 4.2 广播变量引入

~~~
	Spark中分布式执行的代码需要传递到各个executor的task上运行。对于一些只读、固定的数据,每次都需要Driver广播到各个Task上，这样效率低下。广播变量允许将变量只广播给各个executor。该executor上的各个task再从所在节点的BlockManager(负责管理某个executor对应的内存和磁盘上的数据)获取变量，而不是从Driver获取变量，从而提升了效率。
~~~

![广播变量](Spark调优.assets/广播变量.png)

~~~
广播变量，初始的时候，就在Drvier上有一份副本。通过在Driver把共享数据转换成广播变量。

	task在运行的时候，想要使用广播变量中的数据，此时首先会在自己本地的Executor对应的BlockManager中，尝试获取变量副本；如果本地没有，那么就从Driver远程拉取广播变量副本，并保存在本地的BlockManager中；
	
	此后这个executor上的task，都会直接使用本地的BlockManager中的副本。那么这个时候所有该executor中的task都会使用这个广播变量的副本。也就是说一个executor只需要在第一个task启动时，获得一份广播变量数据，之后的task都从本节点的BlockManager中获取相关数据。

	executor的BlockManager除了从driver上拉取，也可能从其他节点的BlockManager上拉取变量副本，网络距离越近越好。
~~~

## 4.3 使用广播变量后的性能分析

~~~
比如一个任务需要50个executor，1000个task，共享数据为100M。
(1)在不使用广播变量的情况下，1000个task，就需要该共享数据的1000个副本，也就是说有1000份数需要大量的网络传输和内存开销存储。耗费的内存大小1000*100=100G.

(2)使用了广播变量后，50个executor就只需要50个副本数据，而且不一定都是从Driver传输到每个节点，还可能是就近从最近的节点的executor的blockmanager上拉取广播变量副本，网络传输速度大大增加；内存开销 50*100M=5G

总结：
	不使用广播变量的内存开销为100G，使用后的内存开销5G，这里就相差了20倍左右的网络传输性能损耗和内存开销，使用广播变量后对于性能的提升和影响，还是很可观的。
	
	广播变量的使用不一定会对性能产生决定性的作用。比如运行30分钟的spark作业，可能做了广播变量以后，速度快了2分钟，或者5分钟。但是一点一滴的调优，积少成多。最后还是会有效果的。
~~~



## 4.4 广播变量使用注意事项

~~~
（1）能不能将一个RDD使用广播变量广播出去？

       不能，因为RDD是不存储数据的。可以将RDD的结果广播出去。

（2）广播变量只能在Driver端定义，不能在Executor端定义。

（3）在Driver端可以修改广播变量的值，在Executor端无法修改广播变量的值。

（4）如果executor端用到了Driver的变量，如果不使用广播变量在Executor有多少task就有多少Driver端的变量副本。

（5）如果Executor端用到了Driver的变量，如果使用广播变量在每个Executor中只有一份Driver端的变量副本。
~~~



## 4.5 如何使用广播变量

* 例如

~~~
(1) 通过sparkContext的broadcast方法把数据转换成广播变量，类型为Broadcast，
	val broadcastArray: Broadcast[Array[Int]] = sc.broadcast(Array(1,2,3,4,5,6))
	
(2) 然后executor上的BlockManager就可以拉取该广播变量的副本获取具体的数据。
		获取广播变量中的值可以通过调用其value方法
	 val array: Array[Int] = broadcastArray.value
~~~

# 5. 尽量避免使用shuffle类算子

## 5.1 shuffle描述

~~~
	spark中的shuffle涉及到数据要进行大量的网络传输，下游阶段的task任务需要通过网络拉取上阶段task的输出数据，shuffle过程，简单来说，就是将分布在集群中多个节点上的同一个key，拉取到同一个节点上，进行聚合或join等操作。比如reduceByKey、join等算子，都会触发shuffle操作。
	
	如果有可能的话，要尽量避免使用shuffle类算子。
	因为Spark作业运行过程中，最消耗性能的地方就是shuffle过程。
	
~~~

## 5.2 哪些算子操作会产生shuffle

~~~
	spark程序在开发的过程中使用reduceByKey、join、distinct、repartition等算子操作，这里都会产生shuffle，由于shuffle这一块是非常耗费性能的，实际开发中尽量使用map类的非shuffle算子。这样的话，没有shuffle操作或者仅有较少shuffle操作的Spark作业，可以大大减少性能开销。
~~~



## 5.3 如何避免产生shuffle

* 小案例

~~~scala
//错误的做法：
// 传统的join操作会导致shuffle操作。
// 因为两个RDD中，相同的key都需要通过网络拉取到一个节点上，由一个task进行join操作。
val rdd3 = rdd1.join(rdd2)
    
//正确的做法：
// Broadcast+map的join操作，不会导致shuffle操作。
// 使用Broadcast将一个数据量较小的RDD作为广播变量。
val rdd2Data = rdd2.collect()
val rdd2DataBroadcast = sc.broadcast(rdd2Data)

// 在rdd1.map算子中，可以从rdd2DataBroadcast中，获取rdd2的所有数据。
// 然后进行遍历，如果发现rdd2中某条数据的key与rdd1的当前数据的key是相同的，那么就判定可以进行join。
// 此时就可以根据自己需要的方式，将rdd1当前数据与rdd2中可以连接的数据，拼接在一起（String或Tuple）。
val rdd3 = rdd1.map(rdd2DataBroadcast...)

// 注意，以上操作，建议仅仅在rdd2的数据量比较少（比如几百M，或者一两G）的情况下使用。
// 因为每个Executor的内存中，都会驻留一份rdd2的全量数据。
~~~



## 5.4 使用map-side预聚合的shuffle操作

* map-side预聚合

~~~
	如果因为业务需要，一定要使用shuffle操作，无法用map类的算子来替代，那么尽量使用可以map-side预聚合的算子。

	所谓的map-side预聚合，说的是在每个节点本地对相同的key进行一次聚合操作，类似于MapReduce中的本地combiner。
	map-side预聚合之后，每个节点本地就只会有一条相同的key，因为多条相同的key都被聚合起来了。其他节点在拉取所有节点上的相同key时，就会大大减少需要拉取的数据数量，从而也就减少了磁盘IO以及网络传输开销。
	通常来说，在可能的情况下，建议使用reduceByKey或者aggregateByKey算子来替代掉groupByKey算子。因为reduceByKey和aggregateByKey算子都会使用用户自定义的函数对每个节点本地的相同key进行预聚合。
	而groupByKey算子是不会进行预聚合的，全量的数据会在集群的各个节点之间分发和传输，性能相对来说比较差。
	
	比如如下两幅图，就是典型的例子，分别基于reduceByKey和groupByKey进行单词计数。其中第一张图是groupByKey的原理图，可以看到，没有进行任何本地聚合时，所有数据都会在集群节点之间传输；第二张图是reduceByKey的原理图，可以看到，每个节点本地的相同key数据，都进行了预聚合，然后才传输到其他节点上进行全局聚合。
~~~

* ==groupByKey进行单词计数原理==

![1577080609633](Spark调优.assets/groupByKey.png)



* ==reduceByKey单词计数原理==

![1577080686083](Spark调优.assets/reduceByKey.png)



# 6. 使用高性能的算子



## 6.1 使用reduceByKey/aggregateByKey替代groupByKey

* reduceByKey/aggregateByKey 可以进行预聚合操作，减少数据的传输量，提升性能
* groupByKey 不会进行预聚合操作，进行数据的全量拉取，性能比较低



## 6.2 使用mapPartitions替代普通map

~~~
	mapPartitions类的算子，一次函数调用会处理一个partition所有的数据，而不是一次函数调用处理一条，性能相对来说会高一些。
	但是有的时候，使用mapPartitions会出现OOM（内存溢出）的问题。因为单次函数调用就要处理掉一个partition所有的数据，如果内存不够，垃圾回收时是无法回收掉太多对象的，很可能出现OOM异常。所以使用这类操作时要慎重！
~~~



## 6.3 使用foreachPartition替代foreach

~~~
	原理类似于“使用mapPartitions替代map”，也是一次函数调用处理一个partition的所有数据，而不是一次函数调用处理一条数据。
	在实践中发现，foreachPartitions类的算子，对性能的提升还是很有帮助的。比如在foreach函数中，将RDD中所有数据写MySQL，那么如果是普通的foreach算子，就会一条数据一条数据地写，每次函数调用可能就会创建一个数据库连接，此时就势必会频繁地创建和销毁数据库连接，性能是非常低下；	但是如果用foreachPartitions算子一次性处理一个partition的数据，那么对于每个partition，只要创建一个数据库连接即可，然后执行批量插入操作，此时性能是比较高的。实践中发现，对于1万条左右的数据量写MySQL，性能可以提升30%以上。
~~~



## 6.4 使用filter之后进行coalesce操作

~~~
	通常对一个RDD执行filter算子过滤掉RDD中较多数据后（比如30%以上的数据），建议使用coalesce算子，手动减少RDD的partition数量，将RDD中的数据压缩到更少的partition中去。
	因为filter之后，RDD的每个partition中都会有很多数据被过滤掉，此时如果照常进行后续的计算，其实每个task处理的partition中的数据量并不是很多，有一点资源浪费，而且此时处理的task越多，可能速度反而越慢。
	因此用coalesce减少partition数量，将RDD中的数据压缩到更少的partition之后，只要使用更少的task即可处理完所有的partition。在某些场景下，对于性能的提升会有一定的帮助。
~~~



## 6.5 使用repartitionAndSortWithinPartitions替代repartition与sort类操作

~~~
	repartitionAndSortWithinPartitions是Spark官网推荐的一个算子，官方建议，如果需要在repartition重分区之后，还要进行排序，建议直接使用repartitionAndSortWithinPartitions算子。
	因为该算子可以一边进行重分区的shuffle操作，一边进行排序。shuffle与sort两个操作同时进行，比先shuffle再sort来说，性能可能是要高的。
~~~



# 7. 使用Kryo优化序列化性能

## 7.1 spark序列化介绍

~~~
	Spark在进行任务计算的时候，会涉及到数据跨进程的网络传输、数据的持久化，这个时候就需要对数据进行序列化。Spark默认采用Java的序列化器。默认java序列化的优缺点如下:
其好处：
	处理起来方便，不需要我们手动做其他操作，只是在使用一个对象和变量的时候，需要实现Serializble接口。
其缺点：
	默认的序列化机制的效率不高，序列化的速度比较慢；序列化以后的数据，占用的内存空间相对还是比较大。

Spark支持使用Kryo序列化机制。Kryo序列化机制，比默认的Java序列化机制，速度要快，序列化后的数据要更小，大概是Java序列化机制的1/10。所以Kryo序列化优化以后，可以让网络传输的数据变少；在集群中耗费的内存资源大大减少。

~~~

## 7.2 Kryo序列化启用后生效的地方

~~~
Kryo序列化机制，一旦启用以后，会生效的几个地方：
（1）算子函数中使用到的外部变量
	算子中的外部变量可能来着与driver需要涉及到网络传输，就需要用到序列化。
	    最终可以优化网络传输的性能，优化集群中内存的占用和消耗
		
（2）持久化RDD时进行序列化，StorageLevel.MEMORY_ONLY_SER
	将rdd持久化时，对应的存储级别里，需要用到序列化。
	    最终可以优化内存的占用和消耗；持久化RDD占用的内存越少，task执行的时候，创建的对象，就不至于频繁的占满内存，频繁发生GC。
		
（3）	产生shuffle的地方，也就是宽依赖
	下游的stage中的task，拉取上游stage中的task产生的结果数据，跨网络传输，需要用到序列化。最终可以优化网络传输的性能
	
	
~~~



## 7.3 如何开启Kryo序列化机制

~~~scala
// 创建SparkConf对象。
val conf = new SparkConf().setMaster(...).setAppName(...)
// 设置序列化器为KryoSerializer。
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

// 注册要序列化的自定义类型。
conf.registerKryoClasses(Array(classOf[MyClass1], classOf[MyClass2]))
~~~



# 8. 使用fastutil优化数据格式

## 8.1 fastutil介绍

```
fastutil是扩展了Java标准集合框架（Map、List、Set；HashMap、ArrayList、HashSet）的类库，提供了特殊类型的map、set、list和queue；

fastutil能够提供更小的内存占用，更快的存取速度；我们使用fastutil提供的集合类，来替代自己平时使用的JDK的原生的Map、List、Set.
```

## 8.2 fastutil好处

```
fastutil集合类，可以减小内存的占用，并且在进行集合的遍历、根据索引（或者key）获取元素的值和设置元素的值的时候，提供更快的存取速度
```

## 8.3 Spark中应用fastutil的场景和使用

### 8.3.1 算子函数使用了外部变量

```
（1）你可以使用Broadcast广播变量优化；

（2）可以使用Kryo序列化类库，提升序列化性能和效率；

（3）如果外部变量是某种比较大的集合，那么可以考虑使用fastutil改写外部变量；

首先从源头上就减少内存的占用(fastutil)，通过广播变量进一步减少内存占用，再通过Kryo序列化类库进一步减少内存占用。
```

### 8.3.2 算子函数里使用了比较大的集合Map/List

```
在你的算子函数里，也就是task要执行的计算逻辑里面，如果有逻辑中，出现，要创建比较大的Map、List等集合，
可能会占用较大的内存空间，而且可能涉及到消耗性能的遍历、存取等集合操作； 
那么此时，可以考虑将这些集合类型使用fastutil类库重写，

使用了fastutil集合类以后，就可以在一定程度上，减少task创建出来的集合类型的内存占用。 
避免executor内存频繁占满，频繁唤起GC，导致性能下降。
```



### 8.3.3 fastutil的使用

```
第一步：在pom.xml中引用fastutil的包
    <dependency>
      <groupId>fastutil</groupId>
      <artifactId>fastutil</artifactId>
      <version>5.0.9</version>
    </dependency>
    
第二步：平时使用List （Integer）的替换成IntList即可。 
	List<Integer>的list对应的到fastutil就是IntList类型
	
	
使用说明：
基本都是类似于IntList的格式，前缀就是集合的元素类型； 
特殊的就是Map，Int2IntMap，代表了key-value映射的元素类型。
```



# 9. 调节数据本地化等待时长

~~~
	Spark在Driver上对Application的每一个stage的task进行分配之前，都会计算出每个task要计算的是哪个分片数据，RDD的某个partition；Spark的task分配算法，优先会希望每个task正好分配到它要计算的数据所在的节点，这样的话就不用在网络间传输数据；

	但是通常来说，有时事与愿违，可能task没有机会分配到它的数据所在的节点，为什么呢，可能那个节点的计算资源和计算能力都满了；所以这种时候，通常来说，Spark会等待一段时间，默认情况下是3秒（不是绝对的，还有很多种情况，对不同的本地化级别，都会去等待），到最后实在是等待不了了，就会选择一个比较差的本地化级别，比如说将task分配到距离要计算的数据所在节点比较近的一个节点，然后进行计算。
~~~



## 9.1 本地化级别

```
（1）PROCESS_LOCAL：进程本地化
	代码和数据在同一个进程中，也就是在同一个executor中；计算数据的task由executor执行，数据在executor的BlockManager中；性能最好
（2）NODE_LOCAL：节点本地化
	代码和数据在同一个节点中；比如说数据作为一个HDFS block块，就在节点上，而task在节点上某个executor中运行；或者是数据和task在一个节点上的不同executor中；数据需要在进程间进行传输；性能其次
（3）RACK_LOCAL：机架本地化	
	数据和task在一个机架的两个节点上；数据需要通过网络在节点之间进行传输； 性能比较差
（4）	ANY：无限制
	数据和task可能在集群中的任何地方，而且不在一个机架中；性能最差
	
```

## 9.2 数据本地化等待时长

```
spark.locality.wait，默认是3s
首先采用最佳的方式，等待3s后降级,还是不行，继续降级...,最后还是不行，只能够采用最差的。

```

## 9.3 如何调节参数并且测试

```
修改spark.locality.wait参数，默认是3s，可以增加

下面是每个数据本地化级别的等待时间，默认都是跟spark.locality.wait时间相同，
默认都是3s(可查看spark官网对应参数说明，如下图所示)
spark.locality.wait.node
spark.locality.wait.process
spark.locality.wait.rack

```

![data-local-spark](Spark调优.assets/data-local-spark.png)



```
在代码中设置：
new SparkConf().set("spark.locality.wait","10")

然后把程序提交到spark集群中运行，注意观察日志，spark作业的运行日志，推荐大家在测试的时候，先用client模式，在本地就直接可以看到比较全的日志。 
日志里面会显示，starting task .... PROCESS LOCAL、NODE LOCAL.....
例如：
Starting task 0.0 in stage 1.0 (TID 2, 192.168.200.102, partition 0, NODE_LOCAL, 5254 bytes)

观察大部分task的数据本地化级别 
如果大多都是PROCESS_LOCAL，那就不用调节了。如果是发现，好多的级别都是NODE_LOCAL、ANY，那么最好就去调节一下数据本地化的等待时长。应该是要反复调节，每次调节完以后，再来运行，观察日志 
看看大部分的task的本地化级别有没有提升；看看整个spark作业的运行时间有没有缩短。

注意注意：
在调节参数、运行任务的时候，别本末倒置，本地化级别倒是提升了， 但是因为大量的等待时长，spark作业的运行时间反而增加了，那就还是不要调节了。
```



# 10. 基于Spark内存模型调优

## 10.1 spark中executor内存划分

* Executor的内存主要分为三块
  * 第一块是让task执行我们自己编写的代码时使用；
  * 第二块是让task通过shuffle过程拉取了上一个stage的task的输出后，进行聚合等操作时使用
  * 第三块是让RDD缓存时使用

## 10.2 spark的内存模型

~~~
	在spark1.6版本以前 spark的executor使用的静态内存模型，但是在spark1.6开始，多增加了一个统一内存模型。
	通过spark.memory.useLegacyMode 这个参数去配置
			默认这个值是false，代表用的是新的动态内存模型；
			如果想用以前的静态内存模型，那么就要把这个值改为true。
~~~



### 10.2.1 静态内存模型

![1570604272790](Spark调优.assets/1570604272790.png)

~~~
实际上就是把我们的一个executor分成了三部分，
	一部分是Storage内存区域，
	一部分是execution区域，
	还有一部分是其他区域。如果使用的静态内存模型，那么用这几个参数去控制：
	
spark.storage.memoryFraction：默认0.6
spark.shuffle.memoryFraction：默认0.2  
所以第三部分就是0.2

如果我们cache数据量比较大，或者是我们的广播变量比较大，
	那我们就把spark.storage.memoryFraction这个值调大一点。
	但是如果我们代码里面没有广播变量，也没有cache，shuffle又比较多，那我们要把spark.shuffle.memoryFraction 这值调大。
~~~

* 静态内存模型的缺点

~~~
我们配置好了Storage内存区域和execution区域后，我们的一个任务假设execution内存不够用了，但是它的Storage内存区域是空闲的，两个之间不能互相借用，不够灵活，所以才出来我们新的统一内存模型。
~~~



### 10.2.2 统一内存模型

![img](Spark调优.assets/image2018-11-1_16-39-33.png)

~~~
	动态内存模型先是预留了300m内存，防止内存溢出。动态内存模型把整体内存分成了两部分，
由这个参数表示spark.memory.fraction 这个指的默认值是0.6 代表另外的一部分是0.4,

然后spark.memory.fraction 这部分又划分成为两个小部分。这两小部分共占整体内存的0.6 .这两部分其实就是：Storage内存和execution内存。由spark.memory.storageFraction 这个参数去调配，因为两个共占0.6。如果spark.memory.storageFraction这个值配的是0.5,那说明这0.6里面 storage占了0.5，也就是executor占了0.3 。
~~~



* 统一内存模型有什么特点呢?

  ~~~
  Storage内存和execution内存 可以相互借用。不用像静态内存模型那样死板，但是是有规则的
  ~~~



  * ==场景一==

    * Execution使用的时候发现内存不够了，然后就会把storage的内存里的数据驱逐到磁盘上。

      ![1570604662552](Spark调优.assets/1570604662552.png)


  * ==场景二==
    * 一开始execution的内存使用得不多，但是storage使用的内存多，所以storage就借用了execution的内存，但是后来execution也要需要内存了，这个时候就会把storage的内存里的数据写到磁盘上，腾出内存空间。

![1570604675176](Spark调优.assets/1570604675176.png)

~~~
为什么受伤的都是storage呢？

是因为execution里面的数据是马上就要用的，而storage里的数据不一定马上就要用。
~~~

### 10.2.3 任务提交脚本参考

* 以下是一份spark-submit命令的示例，大家可以参考一下，并根据自己的实际情况进行调节

```
bin/spark-submit \
  --master yarn-cluster \
  --num-executors 100 \
  --executor-memory 6G \
  --executor-cores 4 \
  --driver-memory 1G \
  --conf spark.default.parallelism=1000 \
  --conf spark.storage.memoryFraction=0.5 \
  --conf spark.shuffle.memoryFraction=0.3 \
```

### 10.2.4 个人经验

~~~java
java.lang.OutOfMemoryError
ExecutorLostFailure
Executor exit code 为143
executor lost
hearbeat time out
shuffle file lost

如果遇到以上问题，很有可能就是内存除了问题，可以先尝试增加内存。如果还是解决不了，那么请听下一次数据倾斜调优的课。
~~~





