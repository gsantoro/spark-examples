package gs.spark.util

package gs.spark.util

import org.joda.time.Duration
import org.joda.time.format.PeriodFormatterBuilder

object Utils {

  def time(execution: () => Unit): Unit = {
    val start = System.currentTimeMillis()

    execution()

    val end = System.currentTimeMillis()

    val duration = new Duration(end - start)
    val formatter = new PeriodFormatterBuilder()
      .appendDays()
      .appendSuffix("d")
      .appendHours()
      .appendSuffix("h")
      .appendMinutes()
      .appendSuffix("m")
      .appendSeconds()
      .appendSuffix("s")
      .toFormatter()
    val formatted = formatter.print(duration.toPeriod())
    println(s"Execution time: $formatted")
  }

}