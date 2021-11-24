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
   *
   * spark sql cache() 默认的存储级别是MemoryAndDisk
   * spark rdd cache() 默认的存储级别是Memory
   */


  /**
   * 1. 创建dataframe的几种方式
   * 1.1 定义schema
   *  val schema = StructType(Array(StructField("user_id", StringType, true),
   * StructField("locale", StringType, true),StructField("birthyear", StringType, true),
   * StructField("gender",StringType, true), StructField("joinedAt", StringType, true),
   * StructField("location", StringType, true), StructField("timezone", StringType, true)))
   * 1.2 定义case class
   * case class resultset(masterhotel:Int,quantity:Double,date:String,rank:Int,frcst_cii:Double,hotelid:Int)
   *
   * 2. 常用的dataframe操作
   * 2.1 基本操作
   * 1、 cache()同步数据的内存
   * 2、 columns 返回一个string类型的数组，返回值是所有列的名字
   * 3、 dtypes返回一个string类型的二维数组，返回值是所有列的名字以及类型
   * 4、 explain()打印执行计划  物理的
   * 5、 explain(n:Boolean) 输入值为 false 或者true ，返回值是unit  默认是false ，如果输入true 将会打印 逻辑的和物理的
   * 6、 isLocal 返回值是Boolean类型，如果允许模式是local返回true 否则返回false
   * 7、 persist(newlevel:StorageLevel) 返回一个dataframe.this.type 输入存储模型类型
   * 8、 printSchema() 打印出字段名称和类型 按照树状结构来打印
   * 9、 registerTempTable(tablename:String) 返回Unit ，将df的对象只放在一张表里面，这  个表随着对象的删除而删除了
   * 10、 schema 返回structType 类型，将字段名称和类型按照结构体类型返回
   * 11、 toDF()返回一个新的dataframe类型的
   * 12、 toDF(colnames：String*)将参数中的几个字段返回一个新的dataframe类型的，
   * 13、 unpersist() 返回dataframe.this.type 类型，去除模式中的数据
   * 14、 unpersist(blocking:Boolean)返回dataframe.this.type类型 true 和unpersist是一样的作用false 是去除RDD
   *
   * 2.2 聚合查询操作
   * 1、 agg(expers:column*) 返回dataframe类型 ，同数学计算求值
   *       df.agg(max("age"), avg("salary"))
   *       df.groupBy().agg(max("age"), avg("salary"))
   * 2、 agg(exprs: Map[String, String])  返回dataframe类型 ，同数学计算求值 map类型的
   *     df.agg(Map("age" -> "max", "salary" -> "avg"))
   *     df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * 3、 agg(aggExpr: (String, String), aggExprs: (String, String)*)  返回dataframe类型 ，同数学计算求值
   *     df.agg(Map("age" -> "max", "salary" -> "avg"))
   *     df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
   * 4、 apply(colName: String) 返回column类型，捕获输入进去列的对象
   * 5、 as(alias: String) 返回一个新的dataframe类型，就是原来的一个别名
   * 6、 col(colName: String)  返回column类型，捕获输入进去列的对象
   * 7、 cube(col1: String, cols: String*) 返回一个GroupedData类型，根据某些字段来汇总
   * 8、 distinct 去重 返回一个dataframe类型
   * 9、 drop(col: Column) 删除某列 返回dataframe类型
   * 10、 dropDuplicates(colNames: Array[String]) 删除相同的列 返回一个dataframe
   * 11、 except(other: DataFrame) 返回一个dataframe，返回在当前集合存在的在其他集合不存在的
   * 12、 explode[A, B](inputColumn: String, outputColumn: String)(f: (A) ⇒ TraversableOnce[B])(implicit arg0: scala.reflect.api.JavaUniverse.TypeTag[B]) 返回值是dataframe类型，这个 将一个字段进行更多行的拆分
   *     df.explode("name","names") {name :String=> name.split(" ")}.show();
   * 将name字段根据空格来拆分，拆分的字段放在names里面
   * 13、 filter(conditionExpr: String): 刷选部分数据，返回dataframe类型 df.filter("age>10").show();  df.filter(df("age")>10).show();   df.where(df("age")>10).show(); 都可以
   * 14、 groupBy(col1: String, cols: String*) 根据某写字段来汇总返回groupedate类型   df.groupBy("age").agg(Map("age" ->"count")).show();df.groupBy("age").avg().show();都可以
   * 15、 intersect(other: DataFrame) 返回一个dataframe，在2个dataframe都存在的元素
   * 16、 join(right: DataFrame, joinExprs: Column, joinType: String)
   * 一个是关联的dataframe，第二个关联的条件，第三个关联的类型：inner, outer, left_outer, right_outer, leftsemi
   *     df.join(ds,df("name")===ds("name") and  df("age")===ds("age"),"outer").show();
   * 17、 limit(n: Int) 返回dataframe类型  去n 条数据出来
   * 18、 na: DataFrameNaFunctions ，可以调用dataframenafunctions的功能区做过滤 df.na.drop().show(); 删除为空的行
   * 19、 orderBy(sortExprs: Column*) 做alise排序
   * 20、 select(cols:string*) dataframe 做字段的刷选 df.select($"colA", $"colB" + 1)
   * 21、 selectExpr(exprs: String*) 做字段的刷选 df.selectExpr("name","name as names","upper(name)","age+1").show();
   * 22、 sort(sortExprs: Column*) 排序 df.sort(df("age").desc).show(); 默认是asc
   * 23、 unionAll(other:Dataframe) 合并 df.unionAll(ds).show();
   * 24、 withColumnRenamed(existingName: String, newName: String) 修改列表 df.withColumnRenamed("name","names").show();
   * 25、 withColumn(colName: String, col: Column) 增加一列 df.withColumn("aa",df("name")).show();
   *
   * 2.3 行动操作
   * 1、 collect() ,返回值是一个数组，返回dataframe集合所有的行
   * 2、 collectAsList() 返回值是一个java类型的数组，返回dataframe集合所有的行
   * 3、 count() 返回一个number类型的，返回dataframe集合的行数
   * 4、 describe(cols: String*) 返回一个通过数学计算的类表值(count, mean, stddev, min, and max)，这个可以传多个参数，中间用逗号分隔，如果有字段为空，那么不参与运算，只这对数值类型的字段。例如df.describe("age", "height").show()
   * 5、 first() 返回第一行 ，类型是row类型
   * 6、 head() 返回第一行 ，类型是row类型
   * 7、 head(n:Int)返回n行  ，类型是row 类型
   * 8、 show()返回dataframe集合的值 默认是20行，返回类型是unit
   * 9、 show(n:Int)返回n行，，返回值类型是unit
   * 10、 table(n:Int) 返回n行  ，类型是row 类型
   */
}
