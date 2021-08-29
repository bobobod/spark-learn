package com.cczu.scala.function

object FunctionDemo {
  def main(args: Array[String]): Unit = {
    // 定义函数
    def hello(str: String): String = {
      "hello world"
    }
    // 存在相同的函数名，类名
    println(hello("hell"))
    println(FunctionDemo.hello("hell"))

    // 可变参数
    // 如果有多个参数的话，可变参数必须在最后
    def func1(str1: String, str: String*): Unit = {
      println(str1, str)
    }

    func1("111", "222", "333")

    // 带默认值
    def func2(str: String = "hello world"): Unit = {
      println(str)
    }

    func2()

    // 带名参数
    def func3(name: String, age: Int): Unit = {
      println(s"$age $name")
    }

    func3(name = "hello", age = 11)

    // 函数至简原则
    /**
     * 1.return 可以省略
     * 2. 函数体只有一行代码，花括号可以省略
     * 3. 如果值可以推断出来，返回值的：和返回值类型可以省略
     * 4. 如果有return，返回值必须指定
     * 5. 如果函数申明Unit，函数体内的return不起作用
     * 6. 如果期望是无返回值类型，可以省略等号
     * 7. 如果函数无参，但是申明了参数列表，那么调用的时候，括号可嫁可不加
     * 8. 如果函数没有参数，那么小括号可以省略，调用时小括号必须省略
     * 9. 如果不关系函数名称，只注重逻辑，那么def也可以省略
     */
    // 6
    def func4(name: String) {
      println(name)
    }

    func4("hello")

    // 7
    def func5() {
      println("name")
    }

    func5

    // 8
    def func6 {
      println("name")
    }

    func6
    // 9 匿名函数
    val func7 = (name: String) => {
      println(name)
    }

    func7("hello")
  }

  // 定义方法
  def hello(str: String): String = {
    "outer hello world"
  }
}
