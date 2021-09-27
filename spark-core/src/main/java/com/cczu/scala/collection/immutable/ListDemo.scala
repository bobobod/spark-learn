package com.cczu.scala.collection.immutable

object ListDemo {
  def main(args: Array[String]): Unit = {
    /**
     * scala.collection.Seq         => java.util.List
     *
     * scala.collection.mutable.Seq => java.util.List
     *
     * scala.collection.Set         => java.util.Set
     *
     * scala.collection.Map         => java.util.Map
     *
     * java.util.Properties         => scala.collection.mutable.Map[String, String]
     */

    // sealed 所有当前类的子类都得放在文件里
    val list: List[Int] = List(23, 23, 11)
    println(list(1))
    //    list(1) =1 error
    list.foreach(println)
    // 3. 添加元素 前加
    val list2: List[Int] = list.+:(11)
    // 后加
    val list3: List[Int] = list.:+(11)
    println("-------")
    println(list2)
    // List(11, 23, 23, 11)
    println(list3)
    // List(23, 23, 11, 11)
    println(list3.init)
    // List(23, 23, 11)
    val list4 = 72 :: 82 :: Nil
    val list5 = list :: list4
    println(list5)
    // List(List(23, 23, 11), 72, 82)
    // 合并
    val list6 = list ::: list4
    val list7 = list ++ list4
    println(list6)
    //    List(23, 23, 11, 72, 82)


    println(list.length)
    println(list.mkString(","))

    println(list.head)
    println(list.tail)
    // 最后一个元素
    println(list.last)
    // 反转
    val reverse: List[Int] = list.reverse
    // 取前（后）几个元素
    println(list.take(2))
    println(list.takeRight(3))
    // 删除前（后）几个元素
    println(list.drop(2))

    // 并集
    val union: List[Int] = list.union(list2)
    // 交集
    val intersect: List[Int] = list.intersect(list2)
    // 差集
    val diff: List[Int] = list.diff(list2)

    // 拉链
    val zip: List[(Int, Int)] = list.zip(list2)
    // 滑窗
    list.sliding(3, 1).foreach(println)

    // 求和
    println(list.sum)
    // 乘法
    println(list.product)
    //最大值
    println(list.max)
    println(list.min)
    // 排序
    println(list.sorted)
    println(list.sorted(Ordering[Int].reverse))

    // par实现并行处理
    val ints = list.par.filter(_ % 2 == 0).map(_ * 2)
  }

}
