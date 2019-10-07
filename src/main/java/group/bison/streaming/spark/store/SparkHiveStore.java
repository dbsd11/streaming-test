package group.bison.streaming.spark.store;

import com.google.gson.JsonObject;
import group.bison.process.core.model.ProcessNode;
import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.core.store.StreamStore;
import group.bison.streaming.spark.driver.JavaStreamingContextAware;
import group.bison.streaming.spark.driver.SparkDriverContext;
import group.bison.streaming.spark.schema.RowWrapper;
import group.bison.streaming.spark.utils.GsonUtil;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by BSONG on 2019/10/7.
 */
public class SparkHiveStore implements StreamStore<RowWrapper, Void>, JavaStreamingContextAware {

    @Override
    public Void persist(DriverContext driverContext, Long pid, RowWrapper schema) {
        SparkDriverContext sparkDriverContext = (SparkDriverContext) driverContext;
        JavaStreamingContext javaStreamingContext = getStreamingContext(sparkDriverContext);

        ProcessNode storeNode = (ProcessNode) sparkDriverContext.getProperties().get("_store");

        JavaPairDStream javaPairDStream = (JavaPairDStream) sparkDriverContext.getProperties().get("_javaPairDStream");

        JsonObject extObject = GsonUtil.getGson().fromJson(storeNode.getConfig().getExt(), JsonObject.class);
        JsonObject hiveObject = extObject.get("hive").getAsJsonObject();

        String hiveTableName = hiveObject.get("tableName").getAsString();
        String format = hiveObject.get("format").getAsString();
        String connectionName = hiveObject.get("connectionName").getAsString();
        String metastoreURI = hiveObject.get("metastoreURI").getAsString();
        String metastoreWarehouseDir = hiveObject.get("metastoreWarehouseDir").getAsString();
        String dbName = hiveObject.get("dbName").getAsString();

        HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(javaStreamingContext.sparkContext().sc());
        hiveContext.setConf("hive.metastore.uris", metastoreURI);
        hiveContext.setConf("hive.metastore.warehouse.dir", metastoreWarehouseDir);

        return null;
    }
}
