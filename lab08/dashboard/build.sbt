name := "dashboard"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0"

libraryDependencies += "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "6.8.9"
