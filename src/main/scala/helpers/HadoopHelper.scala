package helpers

import org.apache.hadoop.io.{NullWritable, Text, MapWritable}


object HadoopHelper {
  def mapToWritable(in: Map[String, String]): (Object, Object) = {
    val m = new MapWritable
    for ((k, v) <- in)
      m.put(new Text(k), new Text(v))
    (NullWritable.get, m)
  }
}
