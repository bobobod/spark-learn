package com.cczu.scala.obj;

public class DynamicJavaDemo {
    public static void main(String[] args) {
        Person1 person1 = new Worker1();
        System.out.println(person1.name); //person
        person1.print();  // hello worker
    }
}

class Person1 {
    String name = "person";

    public void print() {
        System.out.println("hello person");
    }
}

class Worker1 extends Person1 {
    String name = "worker";

    @Override
    public void print() {
        System.out.println("hello worker");
    }
}