name := "BD_Spark_Project"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq( "org.apache.spark" % "spark-core_2.11" % "2.1.0")

libraryDependencies ++= Seq( "org.apache.spark" % "spark-sql_2.11" % "2.1.0")

libraryDependencies ++= Seq( "au.com.bytecode" % "opencsv" % "2.4")