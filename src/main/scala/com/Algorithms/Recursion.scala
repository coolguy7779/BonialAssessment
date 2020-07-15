package com.Algorithms

object Recursion extends App {
  //val n = 5
  //var a:Int = _
  //var b: Int = _
  var c: Long = _
  var arr = new Array[Long](101)
//  arr(0) = 0
//  arr(1) = 1

  /* Factorial*/
  def fact(n: Int): Int = {

    if (n ==1){
      return 1
    }
     n * fact(n-1)

  }
  //print(fact(4))

  /*Fibonacci
  * f(n) = f(n-1) + f(n-2)*/

  def fib(n:Int) : Long ={
    if (n < 2){
     return n
    }
    if (arr(n) != 0){  /*Memoization - top down approach*/
        return arr(n)  /* Optimization */
    }
    val a = fib(n-1)
    val b = fib(n-2)
    c = a + b
    //c = fib(n-1) + fib(n-2)
    arr(n) = c
    c
      }
time{println(fib(30))}
 // println(fib(50))

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) + " ms")
    result
  }
}
