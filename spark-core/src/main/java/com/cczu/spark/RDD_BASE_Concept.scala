package com.cczu.spark

/**
 *
 * @author jianzhen.yin
 * @date 2021/1/5
 */
object RDD_BASE_Concept {
  /**
   * RDD的作用是封装计算逻辑，并生成task
   *
   * 多个连续的RDD的依赖，称为血缘关系
   * 每个RDD都会保存血缘关系
   * RDD不会保持数据
   * RDD为了容错性，需要将RDD间的关系保持下来，一旦出现错误，根据血缘关系重新读取进行计算
   *
   * 基本概念：
   * RDD 任务切分中间分为:Application、Job、Stage 和 Task
   *
   * 1)Application:初始化一个 SparkContext 即生成一个 Application
   * 2)Job:一个 Action 算子就会生成一个 Job
   * 3)Stage:根据 RDD 之间的依赖关系的不同将 Job 划分成不同的 Stage，遇到一个宽依赖 则划分一个 Stage。
   *  stage：shuffle宽依赖数量+1
   * 4)Task:Stage 是一个 TaskSet，将 Stage 划分的结果发送到不同的 Executor 执行即为一个 Task。
   *  Task：1个stage阶段中最后一个依赖RDD的分区数量就是这个stage的task的个数
   *
   *  计算和数据的位置存在不同的级别，这个级别称为本地化级别
   *  节点本地化：计算和数据在同一个节点上
   *  进程本地化：计算和数据在同一个进程中
   *  机架本地化：计算和数据在同一个机架中
   * 注意:Application->Job->Stage->Task 每一层都是 1 对 n 的关系。
   *
   * RDD缓存：
   * RDD 通过 persist 方法或 cache 方法可以将前面的计算结果缓存，默认情况下 persist() 会把数据以序列化的形式缓存在 JVM 的堆空间中。
   * 但是并不是这两个方法被调用时立即缓存，而是触发后面的 action 时，该 RDD 将会 被缓存在计算节点的内存中，并供后面重用。
   * 缓存有可能丢失，或者存储存储于内存的数据由于内存不足而被删除，RDD 的缓存容 错机制保证了即使缓存丢失也能保证计算的正确执行。
   * 通过基于 RDD 的一系列转换，丢 失的数据会被重算，由于 RDD 的各个 Partition 是相对独立的，因此只需要计算丢失的部 分即可，
   * 并不需要重算全部 Partition。
   *
   * RDD依赖
   * OneToOneDependency 新的RDD的一个分区的数据依赖依赖旧的RDD的一个分区
   * ShuffleDependency 新的RDD的一个分区的数据依赖依赖旧的RDD的多个分区
   *
   * RDD阶段划分
   * 当RDD中存在shuffle依赖时，阶段就会自动加一
   * 阶段数量=shuffle数量+1 （ resultStage只有一个，最后需要执行的阶段）
   *
   * RDD任务划分
   * 最后一个RDD的分区个数就是task的个数
   *
   * 如果一个RDD需要重复使用，那么需要从头执行来获取数据，RDD对象可以重用，数据需要重新获取
   *
   * RDD 持久化
   * cache: 将数据临时存储在内存中进行数据重用
   *        会在血缘关系中会添加新的依赖，一旦出现问题，可以重头读取数据
   * persist：将数据临时存放在内存或文件中进行数据重用
   *        涉及到磁盘IO，性能较低，但是数据安全
   *        如果作业执行完毕，临时保存的数据文件就会丢失
   *        会在血缘关系中会添加新的依赖，一旦出现问题，可以重头读取数据
   * checkpoint：将数据长久保存在磁盘文件中进行数据重用
   *        为了保证数据安全，所以一般情况下，会独立执行作业
   *        为了能够提高，一般和cache联合使用
   *        执行过程中，会切断血缘关系，重新建立新的血缘关系。checkpoint等同于改变了数据源
   *
   * shuffle一定会有落盘，减少落盘数据量很重要，向reduceByKey都有预聚合操作可以有效减少数据量
   *
   * spark 默认读取和保存文件的格式是parquet
   */
}
