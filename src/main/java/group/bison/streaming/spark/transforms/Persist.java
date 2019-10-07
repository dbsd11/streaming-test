package group.bison.streaming.spark.transforms;

import com.google.gson.JsonObject;
import group.bison.process.core.model.ProcessNode;
import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.spark.driver.SparkDriverContext;
import group.bison.streaming.spark.schema.RowWrapper;
import group.bison.streaming.spark.utils.GsonUtil;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * Created by cloudera on 7/5/17.
 */
public class Persist implements SparkTransformation {

    @Override
    public JavaPairDStream transform(DriverContext driverContext, Long pid, JavaPairDStream<String, RowWrapper> inStream) {
        SparkDriverContext sparkDriverContext = (SparkDriverContext) driverContext;

        ProcessNode node = (ProcessNode) sparkDriverContext.getProperties().get("_node");

        JsonObject extObject = GsonUtil.getGson().fromJson(node.getConfig().getExt(), JsonObject.class);
        JsonObject persistObj = extObject.get("default").getAsJsonObject();
        String storageLevelString = persistObj.get("storage-level").getAsString();
        StorageLevel storageLevel = new StorageLevel().fromString(storageLevelString);

        JavaPairDStream<String, RowWrapper> finalDStream = inStream.persist(storageLevel);
        return finalDStream;
    }
}
