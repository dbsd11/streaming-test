package group.bison.streaming.spark.transforms;

import com.google.gson.JsonObject;
import group.bison.process.core.model.ProcessNode;
import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.spark.driver.SparkDriverContext;
import group.bison.streaming.spark.schema.RowWrapper;
import group.bison.streaming.spark.utils.GsonUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

/**
 * Created by cloudera on 7/5/17.
 */
public class Take implements SparkTransformation {

    @Override
    public JavaPairDStream transform(DriverContext driverContext, Long pid, JavaPairDStream<String, RowWrapper> inStream) {
        SparkDriverContext sparkDriverContext = (SparkDriverContext) driverContext;

        ProcessNode node = (ProcessNode) sparkDriverContext.getProperties().get("_node");

        JsonObject extObject = GsonUtil.getGson().fromJson(node.getConfig().getExt(), JsonObject.class);
        JsonObject takeObj = extObject.get("default").getAsJsonObject();
        Integer number = takeObj.get("number-elements").getAsInt();

        JavaDStream<RowWrapper> dStream = inStream.map(s -> s._2);

        JavaDStream<RowWrapper> finalDStream = dStream.transform(new Function<JavaRDD<RowWrapper>, JavaRDD<RowWrapper>>() {
            @Override
            public JavaRDD<RowWrapper> call(JavaRDD<RowWrapper> rddWrapperMessage) throws Exception {
                rddWrapperMessage.take(number);
                return rddWrapperMessage;
            }
        });
        return finalDStream.mapToPair(s -> new Tuple2<String, RowWrapper>(null, s));
    }
}
