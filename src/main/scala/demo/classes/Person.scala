package demo.classes

import demo.traits.{MakeFriends, SayHello}

class Person(var name:String)  extends SayHello with MakeFriends {

  private val a = "test"

  def sayHello (otherName:String): Unit = println("hello , "+otherName)

  override def mkFriends(p: Person): Unit = {
    println("hello (MakeFriends) : "+p.name +", i am " +name)
  }
  def call(): Unit ={
    println(Person.b)
  }
}

object Person{
  private val b = "半生对象私有变量调用"
  def main(args:Array[String]): Unit ={
    val person = new Person("hao")
    person.sayHello("world")
    person.call()
    println("半生类私有变量：", person.a)

  }
}