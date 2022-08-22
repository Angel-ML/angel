package struct2vec.fastdtwUtils

import com.madsync.time.DateTime

/**
 * A timestamp and some optional value
 *
 * @param t timestamp associated with this entry
 * @param v value associated with this element
 */
final case class TimeSeriesElement[T](t: DateTime, v: Option[T]) {}
