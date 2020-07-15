package com.sharath.mycodeforbonial

object CheckIfMethodParamsAreVal extends App {

  def paramCheck(a:Int): Unit ={

    val b = a +5;
    /*a = 2*/ //reassignment to val. So function parameters are val
    print(b)
  }

}
