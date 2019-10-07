package group.bison.streaming.spark.transforms;

import com.google.gson.JsonObject;
import group.bison.process.core.model.ProcessNode;
import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.spark.driver.SparkDriverContext;
import group.bison.streaming.spark.schema.RowWrapper;
import group.bison.streaming.spark.utils.GsonUtil;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * Created by cloudera on 7/7/17.
 */
public class GroupByKey implements SparkTransformation {

    @Override
    public JavaPairDStream transform(DriverContext driverContext, Long pid, JavaPairDStream<String, RowWrapper> inStream) {
        SparkDriverContext sparkDriverContext = (SparkDriverContext) driverContext;

        ProcessNode node = (ProcessNode) sparkDriverContext.getProperties().get("_node");

        JsonObject extObject = GsonUtil.getGson().fromJson(node.getConfig().getExt(), JsonObject.class);
        JsonObject groupByObj = extObject.get("default").getAsJsonObject();
        String operator = groupByObj.get("operator").getAsString();

        if (!operator.equalsIgnoreCase("groupByKey")) {
            return inStream;
        }

        JavaPairDStream<String, RowWrapper> finalDStream = inStream.groupByKey().mapValues(new Function<Iterable<RowWrapper>, RowWrapper>() {
            @Override
            public RowWrapper call(Iterable<RowWrapper> rddWrapperMessage) throws Exception {
                return (RowWrapper) rddWrapperMessage.iterator().next();
            }
        });
        return finalDStream;
    }
}
