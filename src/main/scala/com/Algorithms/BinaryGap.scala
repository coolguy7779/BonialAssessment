package com.Algorithms

object BinaryGap {


  def main(args: Array[String]): Unit = {
    val max_gap = solution(1041)
    println(max_gap)
  }

  def solution(n: Int): Int = {
    val str = n.toBinaryString
   // var currPosition = 0
    var cntArr = new Array[Int](50)
    var k = 0
    var i = 0
    while(i <= str.length-1){
      var count = 0
      //var dm1 = str(i)
      //var dm2 = str(i+1)
      if(i == 0  && str(i)== '1' && str(i+1) == '0'){

        var j = i + 1
        while(j <= str.length-1 && str(j) != '1' ){
          count = count +1
          j = j+1
        }
        if (j == str.length && str(j-1) != '0'){
          cntArr(k) = count
          k = k+1
        }
        else if(j != str.length){
          cntArr(k) = count
          k = k+1
        }
        i = j
      }

      else if(i < str.length-1 && str(i)== '1' && str(i + 1)== '0'){

        var j = i + 1
        while(j <= str.length-1 && str(j) != '1' ){
          count = count +1
          j = j+1
        }
        if (j == str.length && str(j-1) != '0'){
          cntArr(k) = count
          k = k+1
        }
        else  if(j != str.length){
          cntArr(k) = count
          k = k+1
        }
        i = j

      }
      else{
        i = i + 1
      }
    }
    cntArr.max
  }

}
