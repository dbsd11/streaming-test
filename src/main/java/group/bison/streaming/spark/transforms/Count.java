package group.bison.streaming.spark.transforms;

import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.spark.schema.RowWrapper;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

/**
 * Created by cloudera on 7/13/17.
 */
public class Count implements SparkTransformation {

    @Override
    public JavaPairDStream transform(DriverContext driverContext, Long pid, JavaPairDStream inStream) {
        JavaDStream<Long> countDstream = inStream.count();

        return countDstream.mapToPair(count -> new Tuple2<>(null, RowWrapper.convertToWrapperMessage(RowFactory.create(count))));
    }
}
