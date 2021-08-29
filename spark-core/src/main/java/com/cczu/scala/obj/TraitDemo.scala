package com.cczu.scala.obj

object TraitDemo {
  def main(args: Array[String]): Unit = {
    // trait 类似java中的接口
    // trait 不能带构造参数 而 abstract 可以带构造参数
    val student00: Student003 = new Student003
    student00.eat()
  }
}

// 定义一个特质
trait Young {
  // 定义抽象和非抽象方法
  var age: Int
  val name: String = "young"

  // 声明抽象和非抽象方法
  def play(): Unit = {
    println("play")
  }

  def eat(): Unit
}

class Person003 {
  val name: String = "person"
}

class Student003 extends Person003 with Young {
  // 重写冲突的属性
  override val name: String = "student"
  // 如果说父类和子类中存在相同的属性的话，需要在继承类中重写相应的属性
  // 如果是存在相同的方法的话，继承类也需要重写改方法
  // 如果调用了 super.method()的话， 默认是调用的是最后一个trait
  // 如果是钻石问题的,会有所不同
  //  B extends A  C extends A
  //  D extends B with C
  //        A
  //      /   \
  //     B     C
  //      \   /
  //        D
  //  叠加顺序是  D -- >  C -- > B -- > A
  //  可以在调用的时候指定  super[B].xxx
  var age: Int = 18

  override def eat(): Unit = println(s"eat $name")

  override def play(): Unit = println("student play")
}

