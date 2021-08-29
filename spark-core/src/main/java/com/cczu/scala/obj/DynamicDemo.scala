package com.cczu.scala.obj

object DynamicDemo {
  def main(args: Array[String]): Unit = {
    // 在java中属性是静态绑定的，方法是动态绑定的

    // 而在scala中全部都是动态绑定的
    val person: Person = new Worker
    println(person.name)  //worker  而在java里这里是person
    person.hello()       // hello worker
  }

}

class Person {
  val name: String = "person"

  def hello(): Unit = {
    println("hello person")
  }
}

class Worker extends Person {
  // 覆盖父类的属性不能是个变量
  override val name: String = "worker"

  override def hello(): Unit = {
    println("hello worker")
  }
}
