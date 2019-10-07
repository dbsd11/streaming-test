package group.bison.streaming.spark.transforms;

import group.bison.streaming.core.transforms.Transformation;
import group.bison.streaming.spark.driver.JavaStreamingContextAware;
import group.bison.streaming.spark.schema.RowWrapper;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * Created by BSONG on 2019/10/7.
 */
public interface SparkTransformation extends Transformation<JavaPairDStream<String, RowWrapper>, JavaPairDStream>, JavaStreamingContextAware {
}
