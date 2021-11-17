package example.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
 * Usage: MultiBroadcastTest [partitions] [numElem]
 */
object MultiBroadcastTest {
  def main(args: Array[String]): Unit = {

    val spark = if (args.length > 0 && args(0).equals("test"))
      SparkSession.builder
        .master("local[*]")
        .appName("Multi-Broadcast Test")
        .getOrCreate()
      else SparkSession.builder
        .appName("Multi-Broadcast Test")
        .getOrCreate()

      val slices = 2
      val num = 1000000

      val arr1 = new Array[Int](num)
      for (i <- 0 until arr1.length) {
        arr1(i) = i
      }

      val arr2 = new Array[Int](num)
      for (i <- 0 until arr2.length) {
        arr2(i) = i
      }

      val barr1 = spark.sparkContext.broadcast(arr1)
      val barr2 = spark.sparkContext.broadcast(arr2)
      val observedSizes: RDD[(Int, Int)] = spark.sparkContext.parallelize(1 to 10, slices).map { _ =>
        (barr1.value.length, barr2.value.length)
      }
      // Collect the small RDD so we can print the observed sizes locally.
      observedSizes.collect().foreach(i => println(i))

      spark.stop()
    }
  }
