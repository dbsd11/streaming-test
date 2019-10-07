package group.bison.streaming.spark.transforms;

import com.google.gson.JsonObject;
import group.bison.process.core.model.ProcessNode;
import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.spark.driver.SparkDriverContext;
import group.bison.streaming.spark.schema.RowWrapper;
import group.bison.streaming.spark.utils.GsonUtil;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

/**
 * Created by cloudera on 7/7/17.
 */
public class FlatMap implements SparkTransformation {

    @Override
    public JavaPairDStream transform(DriverContext driverContext, Long pid, JavaPairDStream<String, RowWrapper> inStream) {
        SparkDriverContext sparkDriverContext = (SparkDriverContext) driverContext;

        ProcessNode node = (ProcessNode) sparkDriverContext.getProperties().get("_node");

        JsonObject extObject = GsonUtil.getGson().fromJson(node.getConfig().getExt(), JsonObject.class);
        JsonObject flatMapObj = extObject.get("default").getAsJsonObject();
        String operator = flatMapObj.get("operator").getAsString();
        String mapper = flatMapObj.get("flat-mapper").getAsString();
        String executorPlugin = flatMapObj.get("executor-plugin").getAsString();

        JavaPairDStream<String, RowWrapper> finalDStream = null;
        if (mapper.equalsIgnoreCase("IdentityMapper")) {
            finalDStream = inStream;
        } else {
            if (operator.equalsIgnoreCase("FlatMap")) {
                try {
                    Class userClass = Class.forName(executorPlugin);
                    FlatMapFunction function = (FlatMapFunction) userClass.newInstance();
                    finalDStream = inStream.flatMap(function).mapToPair(s -> new Tuple2<String, RowWrapper>(null, (RowWrapper) s));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    Class userClass = Class.forName(executorPlugin);
                    PairFlatMapFunction function = (PairFlatMapFunction) userClass.newInstance();
                    finalDStream = inStream.flatMapToPair(function);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return finalDStream;
    }
}
