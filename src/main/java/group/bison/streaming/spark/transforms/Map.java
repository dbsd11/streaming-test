package group.bison.streaming.spark.transforms;

import com.google.gson.JsonObject;
import group.bison.process.core.model.ProcessNode;
import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.core.transforms.Transformation;
import group.bison.streaming.spark.driver.SparkDriverContext;
import group.bison.streaming.spark.schema.RowWrapper;
import group.bison.streaming.spark.utils.GsonUtil;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * Created by cloudera on 7/7/17.
 */
public class Map implements Transformation<JavaPairDStream<String, RowWrapper>, JavaPairDStream> {

    @Override
    public JavaPairDStream transform(DriverContext driverContext, Long pid, JavaPairDStream<String, RowWrapper> inStream) {
        SparkDriverContext sparkDriverContext = (SparkDriverContext) driverContext;

        ProcessNode node = (ProcessNode) sparkDriverContext.getProperties().get("_node");

        JsonObject extObject = GsonUtil.getGson().fromJson(node.getConfig().getExt(), JsonObject.class);
        JsonObject defaultObj = extObject.get("default").getAsJsonObject();
        String mapper = defaultObj.get("mapper").getAsString();

        JavaPairDStream<String, RowWrapper> finalDStream = null;

        if (mapper.equalsIgnoreCase("IdentityMapper")) {
            finalDStream = inStream;
        } else {
            String executorPlugin = defaultObj.get("executor-plugin").getAsString();
            try {
                Class userClass = Class.forName(executorPlugin);
                Function function = (Function) userClass.newInstance();
                JavaPairDStream<String, Row> prevRowDstream = inStream.mapValues(s -> s.getRow());
                JavaPairDStream<String, Row> rowRdd = prevRowDstream.mapValues(function);
                finalDStream = rowRdd.mapValues(s -> new RowWrapper(s));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return finalDStream;
    }
}
