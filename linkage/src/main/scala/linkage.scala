import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Linkage {
    def isHeader(line: String) = line.contains("id_1")

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Linkage").setMaster("local")
        val sc = new SparkContext(conf)
        val rawblocks = sc.textFile("src/main/resources/linkage_csv")
       
        val noheader = rawblocks.filter(!isHeader(_))
        noheader.take(10).foreach(println)
        
        sc.stop()
    }
}
