package group.bison.streaming.spark.transforms;

import com.google.gson.JsonObject;
import group.bison.process.core.model.ProcessNode;
import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.spark.driver.SparkDriverContext;
import group.bison.streaming.spark.schema.RowWrapper;
import group.bison.streaming.spark.utils.GsonUtil;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * Created by cloudera on 7/7/17.
 */
public class ReduceByKey implements SparkTransformation {

    @Override
    public JavaPairDStream transform(DriverContext driverContext, Long pid, JavaPairDStream<String, RowWrapper> inStream) {
        SparkDriverContext sparkDriverContext = (SparkDriverContext) driverContext;

        ProcessNode node = (ProcessNode) sparkDriverContext.getProperties().get("_node");

        JsonObject extObject = GsonUtil.getGson().fromJson(node.getConfig().getExt(), JsonObject.class);
        JsonObject reduceByKeyObj = extObject.get("default").getAsJsonObject();
        String operator = reduceByKeyObj.get("operator").getAsString();
        String executorPlugin = reduceByKeyObj.get("executor-plugin").getAsString();

        JavaPairDStream<String, RowWrapper> outputDStream = inStream;
        Function2 function2 = null;
        try {
            Class userClass = Class.forName(executorPlugin);
            function2 = (Function2) userClass.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (operator.equalsIgnoreCase("ReduceByKey")) {
            outputDStream = inStream.reduceByKey(function2);
        } else {
            String windowDurationString = reduceByKeyObj.get("window-duration").getAsString();
            String slideDurationString = reduceByKeyObj.get("slide-duration").getAsString();
            Duration windowDuration = new Duration(Long.parseLong(windowDurationString));
            Duration slideDuration = new Duration(Long.parseLong(slideDurationString));

            outputDStream = inStream.reduceByKeyAndWindow(function2, windowDuration, slideDuration);
        }
        return outputDStream;
    }
}
