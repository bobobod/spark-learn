package com.cczu.scala.obj

object AnnoymousDemo {
  def main(args: Array[String]): Unit = {
    // 匿名内部累
    val annoy: PersonAnnoy = new PersonAnnoy {
      override var name: String = "hello"
    }
    println(annoy.name)
  }

}

abstract class PersonAnnoy {
  var name: String
}