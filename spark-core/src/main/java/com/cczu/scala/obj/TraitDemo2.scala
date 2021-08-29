package com.cczu.scala.obj

object TraitDemo2 {
  def main(args: Array[String]): Unit = {
    /**
     * scala 中的类型检查和转换
     * obj.isInstanceOf[T]  :是否某种类型
     * obj.asInstanceOf[T]  :强转类型
     * classOf[T]     : 获取类型的对象
     *
     * case 类，编译后会生成类和伴生对象，同时给属性上默认添加val
     */
    val register: UserRegister = new UserRegister("alice", "pwd")
    register.insert()

  }
}

// 用户类
class User(val name: String, val password: String)

trait UserDao {
  // 相当于注入
  _: User =>
  // 插入数据
  def insert(): Unit = {
    println(s"${this.name} insert to db")
  }
}

// 定义用户注册类
class UserRegister(name: String, password: String) extends User(name, password) with UserDao
