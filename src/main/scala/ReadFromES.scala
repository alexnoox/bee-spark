import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{MapWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.mr.EsInputFormat

object ReadFromES {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("Bee-Spark")
    val sc = new SparkContext(conf)
    sc.setLocalProperty("spark.serializer", classOf[KryoSerializer].getName)
    val esconf = new Configuration()
    esconf.set(ConfigurationOptions.ES_NODES, "vps67962.ovh.net")
    esconf.set(ConfigurationOptions.ES_PORT, "9200")
    esconf.set(ConfigurationOptions.ES_RESOURCE, "fta/customer") // index/type
    esconf.set(ConfigurationOptions.ES_QUERY, "?q=alex")
    val esRDD = sc.newAPIHadoopRDD(esconf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])

    val docCount = esRDD.count()
    println(s"Number of hits: $docCount")
    esRDD.foreach(println)
  }
}
