package com.Algorithms

/*
* Steve has a string of lowercase characters in range ascii[‘a’..’z’]. He wants to reduce the string to its shortest length by doing a series of operations. In each operation he selects a pair of adjacent lowercase letters that match, and he deletes them. For instance, the string aab could be shortened to b in one operation.

Steve’s task is to delete as many characters as possible using this method and print the resulting string. If the final string is empty, print Empty String

Sample Input 0
aaabccddd
Sample Output 0
abd
Sample Input 2
baab
Sample Output 2
Empty String
*/

object SuperReducedString {
  var n = 0
  def superReducedString1(s: String): String = {
      if (s.length == 0) {
        return "Empty String"
      }

    if(s.length-2 >= 0 && s(s.length - n -1 ) == s(s.length - n -2)){

     // s.

    }


""
  }


}
