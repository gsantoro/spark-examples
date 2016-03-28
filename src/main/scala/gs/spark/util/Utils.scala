package gs.spark.util

package gs.spark.util

import org.joda.time.Duration
import org.joda.time.format.PeriodFormatterBuilder

object Utils {
  def time(message: String = "", execution: () => Unit): Unit = {
    val start = System.currentTimeMillis()

    execution()

    val end = System.currentTimeMillis()

    val duration = new Duration(end - start)
    val timeFormatter = new PeriodFormatterBuilder()
      .appendDays()
      .appendSuffix("d")
      .appendHours()
      .appendSuffix("h")
      .appendMinutes()
      .appendSuffix("m")
      .appendSeconds()
      .appendSuffix("s")
      .appendMillis()
      .appendSuffix("sss")
      .toFormatter()
    val formattedTime = timeFormatter.print(duration.toPeriod())

    if (message.isEmpty)
      println(s"Execution time: $formattedTime")
    else
      println(s"$message in $formattedTime")
  }
}