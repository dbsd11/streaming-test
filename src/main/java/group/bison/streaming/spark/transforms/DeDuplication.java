package group.bison.streaming.spark.transforms;

import com.google.gson.JsonObject;
import group.bison.process.core.model.ProcessNode;
import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.spark.driver.SparkDriverContext;
import group.bison.streaming.spark.schema.RowWrapper;
import group.bison.streaming.spark.utils.GsonUtil;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

/**
 * Created by cloudera on 8/23/17.
 */
public class DeDuplication implements SparkTransformation {

    @Override
    public JavaPairDStream transform(DriverContext driverContext, Long pid, JavaPairDStream<String, RowWrapper> inStream) {
        SparkDriverContext sparkDriverContext = (SparkDriverContext) driverContext;

        ProcessNode node = (ProcessNode) sparkDriverContext.getProperties().get("_node");

        JsonObject extObject = GsonUtil.getGson().fromJson(node.getConfig().getExt(), JsonObject.class);
        JsonObject dedupObj = extObject.get("deduplication").getAsJsonObject();
        String type = dedupObj.get("type").getAsString();

        JavaDStream<RowWrapper> dStream = inStream.map(s -> s._2);
        JavaPairDStream<String, RowWrapper> deDupPairDstream = null;


        if (type.equalsIgnoreCase("WindowDeduplication")) {
            String colName = dedupObj.get("windowDeDuplicationColumn").getAsString();
            String windowDurationString = dedupObj.get("windowDuration").getAsString();
            long windowDuration = Long.parseLong(windowDurationString);
            MapToPair mapToPairClass = new MapToPair();
            JavaPairDStream<String, RowWrapper> pairedDstream = mapToPairClass.mapToPair(dStream, colName);
            WindowDeDuplication windowDeDuplication = new WindowDeDuplication();
            deDupPairDstream = windowDeDuplication.convertJavaPairDstream(pairedDstream, windowDuration);

        } else if (type.equalsIgnoreCase("HbaseDeduplication")) {
            String colName = dedupObj.get("hbaseDeDuplicationColumn").getAsString();
            String hbaseConnectionName = dedupObj.get("hbaseConnectionName").getAsString();
            String hbaseTableName = dedupObj.get("hbaseTableName").getAsString();
            String schema = dedupObj.get("schema").getAsString();
            StructType structType = (StructType) StructType.fromJson(schema);

            MapToPair mapToPairClass = new MapToPair();
            JavaPairDStream<String, RowWrapper> pairedDstream = mapToPairClass.mapToPair(dStream, colName);
            HBaseDeDuplication hBaseDeDuplication = new HBaseDeDuplication();
            deDupPairDstream = hBaseDeDuplication.convertJavaPairDstream(sparkDriverContext, pairedDstream, hbaseConnectionName, hbaseTableName, structType);
        }

        return deDupPairDstream;
    }
}
