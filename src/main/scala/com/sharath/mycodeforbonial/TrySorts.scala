package com.sharath.mycodeforbonial

object TrySorts {

  def main(args: Array[String]): Unit = {
    println("This is Bubble sort")
    bubbleSort(Array(23,12,21,77,16,1,45,7)).foreach(f => print(f.toString + " "))
    println("\n\nThis is Selection sort")
    selectionSort(Array(23,12,21,77,16,1,45,7)).foreach(f => print(f.toString + " "))
    println("\n\nThis is Insertion Sort")
    insertionSort(Array(23,12,21,77,16,1,45,7)).foreach(f => print(f.toString + " "))

  }

  def bubbleSort(input: Array[Int]): Array[Int] = {
    var a = input
    for(i <- 0 until input.length - 1){
      for(j <- 0 until input.length - i - 1){
         if (a(j) > a(j+1)){
           val temp = a(j)
           a(j) = a(j+1)
           a(j+1) = temp
         }
      }
    }
    a
  }

  def selectionSort(input: Array[Int]): Array[Int] = {
    var a = input

    for(i <- 0 until input.length ){
      var min_idx = i
      for(j <- i+1 until input.length){
        if (a(j) < a(min_idx)) min_idx = j
      }
      val temp = a(i)
      a(i) = a(min_idx)
      a(min_idx) = temp
    }
    a
  }

  def insertionSort(input: Array[Int]):Array[Int] = {
    var a = input
    //var temp= 0
    for(i <- 1 until input.length){

      val temp = a(i)
      var j = i - 1
      while(j >= 0 && a(j) > temp){
        a(j + 1 ) = a(j)
        j -= 1
      }
      a(j + 1 ) = temp
    }
    a
  }
}
