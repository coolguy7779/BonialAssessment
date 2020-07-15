//package com.sharath.mycodeforbonial
//
//import com.datastax.driver.core.TypeCodec.PrimitiveIntCodec
//import com.datastax.driver.core.{Cluster, ResultSet, Row, TypeCodec}
//import com.google.common.util.concurrent.{ListenableFuture,Futures}
//
//import scala.concurrent.{ExecutionContext, Future, Promise,}
//
//
////import scala.collection.JavaConversions._
////import scala.collection.JavaConverters._
//object ReadFromCassandra {
//  def main(args: Array[String]): Unit = {
//
//    val cluster = Cluster.builder()
//      .addContactPoint("localhost")
//      .withPort(9042)
//      .build()
//      .connect("test")
//
//    val session = cluster
//
//    val a = session.execute("SELECT * FROM emp")
//    val resultSetFuture: ListenableFuture[ResultSet] = session.executeAsync("SELECT * FROM emp")
//    import scala.collection.JavaConverters._
//
//    Futures.addCallback()
//    //val ans = resultSetFuture.map(_.asScala)(ExecutionContext.global)
//
//    //val rs = resultSetFuture.getUninterruptibly
//
//
////    println( a.getColumnDefinitions.getName(0))
////    println( a.getColumnDefinitions.getName(1))
////    println( a.getColumnDefinitions.getName(2))
////    //println(a.all())
////    println(a.getColumnDefinitions.iterator().next().getName)
////    //val d = TypeCodec[String]
////    val b   = a.asScala.map(_.getInt(0))
//
////    print(b)
//
//
//  }
//
//
//
//
//
////  def getValueFromCassandraTable():ResultSet = {
////    session.execute("SELECT * FROM test.emp")
////  }
//
////  val a = session.execute("SELECT * FROM test.emp")
////  val b = a.all()
//}
