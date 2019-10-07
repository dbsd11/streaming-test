package group.bison.streaming.spark.transforms;

import com.google.gson.JsonObject;
import group.bison.process.core.model.ProcessNode;
import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.spark.driver.SparkDriverContext;
import group.bison.streaming.spark.schema.RowWrapper;
import group.bison.streaming.spark.utils.GsonUtil;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * Created by cloudera on 8/6/17.
 */
public class FilterCustom implements SparkTransformation {

    @Override
    public JavaPairDStream transform(DriverContext driverContext, Long pid, JavaPairDStream<String, RowWrapper> inStream) {
        SparkDriverContext sparkDriverContext = (SparkDriverContext) driverContext;

        ProcessNode node = (ProcessNode) sparkDriverContext.getProperties().get("_node");

        JsonObject extObject = GsonUtil.getGson().fromJson(node.getConfig().getExt(), JsonObject.class);
        JsonObject filterObj = extObject.get("filter").getAsJsonObject();
        String executorPlugin = filterObj.get("executor-plugin").getAsString();

        JavaPairDStream<String, RowWrapper> finalDStream = null;

        try {
            Class userClass = Class.forName(executorPlugin);
            Function function = (Function) userClass.newInstance();
            JavaPairDStream<String, Row> inRowDstream = inStream.mapValues(s -> s.getRow());
            JavaPairDStream<String, Row> filteredRowDstream = inRowDstream.filter(function);
            finalDStream = filteredRowDstream.mapValues(s -> new RowWrapper(s));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return finalDStream;
    }
}
