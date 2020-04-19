package com.bigdata.hadoop

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

/**
 * @author sahilgogna on 2020-04-15
 */
object Main extends App {

  // making a confugration object
  val config = new Configuration()

  // adding configuration files of the cluster to the config object
  config.addResource(new Path("/Users/sahilgogna/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  config.addResource(new Path("/Users/sahilgogna/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))

  val fileSystem = FileSystem.get(config)


  // printing effective uri
  println(fileSystem.getUri)
  println(fileSystem.getWorkingDirectory)

  //listing contents of a given directory
  fileSystem.listStatus(new Path("/user/fall2019")).foreach(println)

  // checking if the path exist
  if (fileSystem.exists(new Path("/user/fall2019/sahilgogna"))) println("I found my folder")
  else println("I failed in the previous practice")


  // deleting the folder
  fileSystem.delete(new Path("/user/fall2019/sahilgogna"), true)

  // checking if the folder is deleted successfully
  if (fileSystem.exists(new Path("/user/fall2019/sahilgogna"))) println("Unable to delete folder")
  else println("Deleted successfully")

  // creating the folder again
  fileSystem.mkdirs(new Path("/user/fall2019/sahilgogna"))

  // checking if the folder is created successfully
  if (fileSystem.exists(new Path("/user/fall2019/sahilgogna"))) println("Folder Created")
  else println("Unable to create the folder")

  // creating a subfolder
  fileSystem.mkdirs(new Path("/user/fall2019/sahilgogna/stm"))
  if (fileSystem.exists(new Path("/user/fall2019/sahilgogna/stm"))) println("Folder Created")
  else println("Unable to create the folder")

  // copying files
  fileSystem.copyFromLocalFile(new Path("/Users/sahilgogna/Documents/Big Data College/gtfs_stm/stops.txt"), new Path("/user/fall2019/sahilgogna/stm"))
  fileSystem.copyFromLocalFile(new Path("/Users/sahilgogna/Documents/Big Data College/gtfs_stm/stops.txt"), new Path("/user/fall2019/sahilgogna/stm/stops2.txt"))

  // renaming file
  fileSystem.rename(new Path("/user/fall2019/sahilgogna/stm/stops2.txt"), new Path("/user/fall2019/sahilgogna/stm/stops.csv"))

  // reading csv file and printing few lines of the file
  val inputStream: FSDataInputStream = fileSystem.open(new Path("/user/fall2019/sahilgogna/stm/stops.csv"))
  val data: String = IOUtils.toString(inputStream)
  data.split("\n").slice(0, 5).foreach(println)

}
