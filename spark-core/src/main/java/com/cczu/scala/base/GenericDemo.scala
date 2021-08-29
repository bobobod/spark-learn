package com.cczu.scala.base

object GenericDemo {
  def main(args: Array[String]): Unit = {
    /**
     * class MyList[+T]{} // 协变  son是father的子类，MyList[Son]是MyList[Father]的子类
     * class MyList[-T]{} // 逆变  son是father的子类，MyList[Son]是MyList[Father]的父类
     * class MyList[T]{} // 不变  son是father的子类，MyList[Son]和MyList[Father]无关系
     * class MyList[T<:Person] // 类型上限，可以是person的子类
     * class MyList[T>:Person] // 类型下限,可以是Person，也可以是Person的父类型
     */

    val child :Parent= new Child
    // class Collection[E]{}
    val c1: Collection[Parent] = new Collection[Parent]
    // class Collection[+E]{}
    //  val c2: Collection[Parent] = new Collection[Child]
    // class Collection[-E]{}
    // val c3: Collection[SubChild] = new Collection[Parent]
    // class Collection[E <: Parent]{} 上线
    val c4: Collection[SubChild] = new Collection[SubChild]

  }
}
// 定义继承关系
class Parent{}
class Child extends Parent {}
class SubChild extends Child{}

// 定义一个带范性的集合
class Collection[E <: Parent]{}