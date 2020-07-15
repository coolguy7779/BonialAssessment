package com.Algorithms

object BitonicSequence {

  def solution(a: Array[Int]): Int = {
    // write your code in Scala 2.12

    var b = a.sorted
    var spike_len = b.length
    var no_occur = 1
    for(i <- 0 until b.length-1){
      if(b(i)== b(i+1) ) {
        no_occur += 1
        if (no_occur > 2)
          spike_len = spike_len - (no_occur - 2)
      }
      else
        no_occur = 1;
      }
    spike_len
    }

  def main(args: Array[String]): Unit = {
    // 2,3,3,2,2,2,1 ans - 4
    //1,2 ans - 2
    // 2,5,3,2,4,1 ans = 6
    // 1,11,2,10,4,5,2,1 ans =

    val spike_len = solution(Array(1,11,2,10,4,5,2,1))
    println(spike_len)
  }

//    spike_len=n //length of spike initially set to len of array
//    no_occur=1 //no of times a number occurs in the array
//
//
//    //******array a[] is SORTED******
//
//    for (i=0;i<n-1;i++)
//    {
//      if(a[i]==a[i+1])
//        no_occur+=1;
//      if(no_occur>2)
//        spike_len = spike_len - (no_of_occur-2)
//      else
//        no_occur=1;

  }



