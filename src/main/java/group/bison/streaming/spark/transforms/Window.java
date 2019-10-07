package group.bison.streaming.spark.transforms;

import com.google.gson.JsonObject;
import group.bison.process.core.model.ProcessNode;
import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.spark.driver.SparkDriverContext;
import group.bison.streaming.spark.schema.RowWrapper;
import group.bison.streaming.spark.utils.GsonUtil;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * Created by cloudera on 7/3/17.
 */
public class Window implements SparkTransformation {

    @Override
    public JavaPairDStream transform(DriverContext driverContext, Long pid, JavaPairDStream<String, RowWrapper> inStream) {
        SparkDriverContext sparkDriverContext = (SparkDriverContext) driverContext;

        ProcessNode node = (ProcessNode) sparkDriverContext.getProperties().get("_node");

        JsonObject extObject = GsonUtil.getGson().fromJson(node.getConfig().getExt(), JsonObject.class);
        JsonObject windowObj = extObject.get("default").getAsJsonObject();
        String windowType = windowObj.has("window-type") ? windowObj.get("window-type").getAsString() : "SlidingWindow";
        String windowDurationString = windowObj.get("window-duration").getAsString();
        String slideDurationString = windowObj.get("slide-duration").getAsString();

        Duration windowDuration = new Duration(Long.parseLong(windowDurationString));
        JavaPairDStream<String, RowWrapper> windowDStream = null;
        if (windowType.equalsIgnoreCase("FixedWindow")) {
            windowDStream = inStream.window(windowDuration);
            System.out.println(" Inside FixedWindow");
            windowDStream.print();
        } else {
            Duration slideDuration = new Duration(Long.parseLong(slideDurationString));
            windowDStream = inStream.window(windowDuration, slideDuration);
            System.out.println(" Inside Sliding Window");
            windowDStream.print();
        }
        return windowDStream;
    }
}
