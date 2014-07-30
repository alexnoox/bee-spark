import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{MapWritable, NullWritable, Text}
import org.apache.hadoop.mapred.{FileOutputFormat, FileOutputCommitter, JobConf}

import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.mr.EsOutputFormat

object WriteToES {
  def main(args: Array[String]) {
    // Spark Context setup
    val conf = new SparkConf().setMaster("local").setAppName("Bee-Spark")
    val sc = new SparkContext(conf)
    sc.setLocalProperty("spark.serializer", classOf[KryoSerializer].getName)

    // Elasticsearch-Hadoop setup
    val jobConf = new JobConf(sc.hadoopConfiguration)
    jobConf.setOutputFormat(classOf[EsOutputFormat])
    jobConf.setOutputCommitter(classOf[FileOutputCommitter])
    jobConf.set(ConfigurationOptions.ES_NODES, "vps67962.ovh.net")
    jobConf.set(ConfigurationOptions.ES_PORT, "9200")
    jobConf.set(ConfigurationOptions.ES_RESOURCE, "tweets/tweet") // index/type
    FileOutputFormat.setOutputPath(jobConf, new Path("-"))

    // Writing RDD to ElasticSearch
    val tweet = Map("user" -> "kimchy", "post_date" -> "2009-11-15T14:12:12", "message" -> "trying out Elastic Search")
    val tweets = sc.makeRDD(Seq(tweet))
    val writables = tweets.map(toWritable)
    writables.saveAsHadoopDataset(jobConf)
  }

  def toWritable(in: Map[String, String]): (Object, Object) = {
    val m = new MapWritable
    for ((k, v) <- in)
      m.put(new Text(k), new Text(v))
    (NullWritable.get, m)
  }
}
