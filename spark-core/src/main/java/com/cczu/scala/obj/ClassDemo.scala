package com.cczu.scala.obj

import scala.beans.BeanProperty

object ClassDemo {
  def main(args: Array[String]): Unit = {
    /**
     * scala 无public关键字
     * private 为私有权限，只有类的内部和伴生对象可以使用
     * protected 为受保护权限，scala比java更加严格，同类、子类可以访问 但是同包无法访问
     * private[包名] 添加包访问权限
     */
    val student: Student = new Student
    println(student.age)
    student.age = 11
    println(student.getSex)

    val student1: Student = new Student("abc")
    println(student1)

    val student2: Student1 = new Student1("hello", 11)
    println(s"${student2.age} ${student2.name}")

    val student3: Student2 = new Student2("hello", 11)
    // 无法访问参数
  }
}

// 定义一个类
class Student() {
  // 定义属性 默认都是public,底层实际为private ，显示指定为public会报错
  // 默认初始值可以用 _
  // 需要像java一样使用get set方法 可以制定  @BeanProperty 注解
  private[com] var name: String = "alice"
  var age: Int = _
  @BeanProperty
  var sex: String = _

  def func1(): String = {
    name
  }

  println("主构造方法被调用，最新调用")

  // 声明辅助构造器
  def this(name: String) {
    this() // 直接调用主构造器
    println("辅助构造器被调用")
    this.name = name
  }

}

// 定义一个有形参的class  为class的属性,var 是可变的， val 不可变
class Student1(var name: String, var age: Int)

class Student3(val name: String, val age: Int)

class Student4(var name: String, var age: Int) {
  var school: String = _

  def this(name: String, age: Int, school: String) {
    this(name, age)
    this.school = school
  }
  def print(): Unit ={
    println(s"${name} ${age} ${school}")
  }
}


// 形参无修饰，为局部变量，不是属性
class Student2(name: String, age: Int)
