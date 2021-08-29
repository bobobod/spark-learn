package com.cczu.scala.obj

object ObjectDemo {
  def main(args: Array[String]): Unit = {
    // 单例类 object xx  是为了替代java中的static关键字
    val stu: Student001 = Student001.newStudent("bob", 24)
    stu.print()

    // 使用apply 构建对象
    val stu2: Student001 = Student001("bob1", 222)
    stu2.print()
  }
}

// 定义类 如果需要参数私有化的话，在参数前加一个private
class Student001 private(var name: String, var age: Int) {
  def print(): Unit = {
    println(s"$name $age  ${Student001.school}")
    Student001.hello()
  }
}

// 伴生对象
object Student001 {
  // 静态变量
  val school: String = "cczu"

  // 静态方法
  def hello(): Unit = {
    println("hello student")
  }

  // 定义一个类的创建方法  伴生对象可以相互访问不受任何修饰的影响
  def newStudent(name: String, age: Int): Student001 = new Student001(name, age)

  // apply
  def apply(name: String, age: Int): Student001 = new Student001(name, age)
}
