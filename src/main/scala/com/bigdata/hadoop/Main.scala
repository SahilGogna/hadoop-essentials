package com.bigdata.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * @author sahilgogna on 2020-04-15
 */
object Main extends App {

  val config = new Configuration()
  val fs = FileSystem.get(config)
  fs.listStatus(new Path("/"))
    .foreach(println)

}
