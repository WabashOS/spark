/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

object SimpleApp {
  def main(args: Array[String]) {
    /* Initialization */
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    /* Actual Spark code */
    // val C_path = "./source.c"
    // val H_path = "./source.h"
    val C_path = "hdfs://f16/user/skarandikar/hsource.c"
    val H_path = "hdfs://f16/user/skarandikar/hsource.h"

    /* CData is pretty big, but fits in mem */
    val CData = sc.textFile(C_path, 2).persist(StorageLevel.MEMORY_AND_DISK)
    val CSlog = CData.filter(line => line.contains(";")).count()
    
    /* loading .h's will kick out some of CData's blocks */
    val HData = sc.textFile(H_path, 2).cache()
    val HSlog = HData.filter(line => line.contains(";")).count()

    /* using CData again to force load from disk */
    val CBranch = CData.filter(line => line.contains("if")).count()
    
    println(s"SLOC in C files: $CSlog")
    println(s"SLOC in H files: $HSlog")
    println(s"Branches in C files: $CBranch")

    sc.stop()
  }
}
