package group.bison.streaming.spark.transforms;

import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.spark.schema.RowWrapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * Created by cloudera on 7/7/17.
 */
public class SortByKey implements SparkTransformation {

    @Override
    public JavaPairDStream transform(DriverContext driverContext, Long pid, JavaPairDStream<String, RowWrapper> inStream) {
        JavaPairDStream<String, RowWrapper> outputDStream = inStream.transformToPair(new Function<JavaPairRDD<String, RowWrapper>, JavaPairRDD<String, RowWrapper>>() {
            @Override
            public JavaPairRDD<String, RowWrapper> call(JavaPairRDD<String, RowWrapper> stringWrapperMessageJavaPairRDD) throws Exception {
                return stringWrapperMessageJavaPairRDD.sortByKey();
            }
        });

        return outputDStream;
    }
}
