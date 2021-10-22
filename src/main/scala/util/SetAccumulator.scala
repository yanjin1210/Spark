package util

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class SetAccumulator extends AccumulatorV2[String, mutable.HashSet[String]] {
  private var keySet = mutable.HashSet[String]()

  override def isZero: Boolean = keySet.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashSet[String]] = new SetAccumulator()

  override def reset(): Unit = keySet.clear()

  override def add(key: String): Unit = keySet.add(key)

  override def merge(other: AccumulatorV2[String, mutable.HashSet[String]]): Unit = {
    keySet ++= other.value
  }

  override def value: mutable.HashSet[String] = keySet
}