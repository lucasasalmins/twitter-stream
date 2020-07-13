package StreamingApp

import common.Utilities.{setupLogging, setupTwitter}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object PrintTweets {

  def printTweets() = {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    val ssc = new StreamingContext(
      "local[*]",
      "PrintTweets",
      Seconds(1)
    )

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText())

    // Print out the first ten
    statuses.print()

    // Kick it all off
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    printTweets()
  }
}
