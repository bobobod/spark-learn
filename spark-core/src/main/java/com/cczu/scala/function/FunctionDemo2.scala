package com.cczu.scala.function

object FunctionDemo2 {
  def main(args: Array[String]): Unit = {
    // 定义一个函数，以函数作为参数
    val funcParam: String => Unit = (name: String) => {
      println(name)
    }

    def func1(func: String => Unit): Unit = {
      func("atguigu")
    }

    func1(funcParam)

    // 匿名函数的简化原则
    /**
     * 1.参数类型可以自动推导，可省略
     * func1((name)=>{println(name)})
     * 2. 如果只有一个参数，括号可以省略
     * func1(name=>{println(name)})
     * 3. 函数只有一行，大括号可以省略
     * func1(name=>println(name))
     * 4. 如果参数只出现一次，则参数省略且后面的参数可以用_代替,如果是多个参数，顺序需要保持一致
     * func1(println(_))
     * 5. 如果可以推断出，当前传入的println是一个函数体，而不是调用语句，_号也可以省略
     * func1(println)
     */
    // 最终简化后
    func1(println)

    // 1.函数作为值进行传递
    def func2(a: Int): Int = {
      a
    }

    val f1 = func2 _
    val f2: Int => Int = func2
    println(f1(11))
    println(f2(12))

    // 2.函数作为参数
    def dualEval(op: (Int, Int) => Int, a: Int, b: Int): Int = {
      op(a, b)
    }

    println(dualEval(_ + _, 1, 2))

    // 3.函数作为返回值
    def func3() = {
      def func4(a: Int): Int = {
        println(s"a = $a")
        a
      }
      // 如果不指定返回值类型
      func4 _
      // 如果制定返回值类型
      // func3
    }

    println(func3()(11))
  }
}
