version := "0.1.0"
  
scalaVersion := "2.11.7"

name := "Hello Spark"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1"

 libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.2.0"
 
 libraryDependencies += "com.typesafe.play" %% "play-json" % "2.4.3"
 
 libraryDependencies +=   "commons-dbcp" % "commons-dbcp" % "1.4"
 
 libraryDependencies +=   "org.postgresql" % "postgresql" % "9.4-1201-jdbc41"
 
// enablePlugins(PlayScala)

 dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
)