import org.apache.spark.{ SparkConf, SparkContext }

import java.time.Instant
import java.net.URL

object Sessionize {

  case class Log(time: Long, ip:String, url:String, sessionId:String)

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Usage: Sessionize [input file] [output file] [session wimeout in minutes]")
      return
    }

    // Create the spark context with a spark configuration
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("Sessionize")
    val sc = new SparkContext(sparkConf)

    //session window time in milliseconds
    val sessionTimeout = args(2).toInt * 60000

    //make a new RDD from the log file provided
    val logFileRDD = sc.textFile(args(0))

    //parse a timestamp and return milliseconds
    def getTimeInMillisec(sTimeStamp:String):Long = {
      val inst:Instant  = Instant.parse(sTimeStamp)
      inst.toEpochMilli()
    }

    /** parse logFileRDD records to create Log case class instances
      * we are interested in 3 values only:
      * the first group is for timestamp
      * the second one is to match client's ip
      * the last one to get url
      */
    val pattern = "(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d+Z?)\\s\\S+\\s(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):.+\\s\"(?:[A-Z]+)\\s(\\S+)\\sHTTP/".r
    val logRDD = logFileRDD.map(pattern.findFirstMatchIn(_)).filter(!_.isEmpty).map{ line =>
      val ip = line.get.subgroups(1)
      (ip,
        List(
          Log(
            time = getTimeInMillisec(line.get.subgroups(0)),
            ip = ip,
            url = new URL(line.get.subgroups(2)).getPath,
            sessionId = "" //will assign it later when we sessionize the logs
          )
        )
        )
    }

    //reduce to combine lists of Log instances
    val reducedRDD = logRDD.reduceByKey(_ ++ _)

    //generate random UUID to be used as sessionId
    def generateSessionId():String = {
      java.util.UUID.randomUUID().toString
    }

    def sessionizeLogs(pairs: (String, List[Log])) = {
      val sorted = pairs._2.sortBy(_.time)
      //if the head's record sessionIs is not assigned yet, set it to the generated UUID
      val sortedHead = if (sorted.head.sessionId.isEmpty) sorted.head.copy(sessionId = generateSessionId()) else sorted.head
      //scan from head to tail to define the session boundaries
      sorted.tail.scanLeft(sortedHead) { case (prev, current) =>
        //define if the current and previous log records belongs to the same session
        def timedOut = current.time - prev.time > sessionTimeout
        //if they both belong to the same session, assign the same sessionId as in previous record
        //else - set it to the generated UUID
       current.copy(sessionId =  if (timedOut) generateSessionId() else prev.sessionId)
      }
    }

    //apply sessionizeLogs function to set sessionIds
    val sessionizedRDD = reducedRDD.flatMap(sessionizeLogs)
    
    //cache this RDD as it will be used later
    sessionizedRDD.cache

    val sessionPairRDD = sessionizedRDD.map(log => (log.sessionId, List(log)))

    //now we have sessionized logs as RDD[(String, List[Log])]
    val reducedSessionRDD = sessionPairRDD.reduceByKey(_ ++ _)

    //we can save it for future analysis
    reducedSessionRDD.saveAsTextFile(args(2))

    /**
      * Determine the average session time
      * first, create an RDD to calculate min and max time for each session
      * v._1 - sessionId, then we put session's time 2 times to keep min and max values
      * 0 - this field is to be set as session duration
      */
    val timesRDD = sessionizedRDD.map(log => (log.sessionId, (log.time, log.time, 0, log.ip)))

    val durationRDD = timesRDD.reduceByKey((a, b) => {
      val minTime = Math.min(a._1, b._1)
      val maxTime =  Math.max(a._2, b._2)
      val sessionDuration = maxTime - minTime
      (minTime, maxTime, sessionDuration.toInt, a._4)
    })

    //map to duration and filter out the records with zero duration (one hit sessions)
    val durationMean = durationRDD.map(_._2._3).filter(_ > 0).mean
    println("Average session time: " + (durationMean/1000).toInt +  " seconds")

    /** Calculate unique hits per session
      * create a pair RDD with sessionId as a key and url as a value
      */
    val hitsRDD = sessionizedRDD.map(log => (log.sessionId, log.url))
    val uniqueHitsRDD = hitsRDD.combineByKey(List(_), (x:List[String], y:String) => y :: x, (x:List[String], y:List[String]) => x ::: y)
    val mostHitsPerSession = uniqueHitsRDD.mapValues(_.distinct.size).sortBy(_._2, false).take(10)
    println("Records with most unique hits per session:")
    mostHitsPerSession.foreach(println)

    //ips with longest session time
    val ipDurationRDD = durationRDD.values.sortBy(_._3, false)
    val longestSessions = ipDurationRDD.map(v => (v._4, v._3.toInt/1000)).take(10)
    println("Client IPs with longest session time (IP, session duration (in seconds)):")
    longestSessions.foreach(println)

  }
}
