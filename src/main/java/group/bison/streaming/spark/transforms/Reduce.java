package group.bison.streaming.spark.transforms;

import com.google.gson.JsonObject;
import group.bison.process.core.model.ProcessNode;
import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.spark.driver.SparkDriverContext;
import group.bison.streaming.spark.schema.RowWrapper;
import group.bison.streaming.spark.utils.GsonUtil;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

/**
 * Created by cloudera on 7/7/17.
 */
public class Reduce implements SparkTransformation {

    @Override
    public JavaPairDStream transform(DriverContext driverContext, Long pid, JavaPairDStream<String, RowWrapper> inStream) {
        SparkDriverContext sparkDriverContext = (SparkDriverContext) driverContext;

        ProcessNode node = (ProcessNode) sparkDriverContext.getProperties().get("_node");

        JsonObject extObject = GsonUtil.getGson().fromJson(node.getConfig().getExt(), JsonObject.class);
        JsonObject reduceObj = extObject.get("default").getAsJsonObject();
        String operator = reduceObj.get("operator").getAsString();
        String executorPlugin = reduceObj.get("executor-plugin").getAsString();

        JavaDStream<Tuple2<String, RowWrapper>> outputDStream = null;

        Function2 function2 = null;
        try {
            Class userClass = Class.forName(executorPlugin);
            function2 = (Function2) userClass.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (operator.equalsIgnoreCase("Reduce")) {
            outputDStream = inStream.reduce(function2);
        } else {
            String windowDurationString = reduceObj.get("window-duration").getAsString();
            String slideDurationString = reduceObj.get("slide-duration").getAsString();

            Duration windowDuration = new Duration(Long.parseLong(windowDurationString));
            Duration slideDuration = new Duration(Long.parseLong(slideDurationString));

            outputDStream = inStream.reduceByWindow(function2, windowDuration, slideDuration);
        }

        return outputDStream.mapToPair(s -> new Tuple2<String, RowWrapper>(s._1, s._2));

    }
}
