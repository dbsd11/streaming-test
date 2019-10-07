package group.bison.streaming.spark.transforms;

import com.google.gson.JsonObject;
import group.bison.process.core.model.ProcessNode;
import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.core.transforms.Transformation;
import group.bison.streaming.spark.driver.SparkDriverContext;
import group.bison.streaming.spark.schema.RowWrapper;
import group.bison.streaming.spark.utils.GsonUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

/**
 * Created by cloudera on 7/6/17.
 */
public class MapToPair implements Transformation<JavaPairDStream<String, RowWrapper>, JavaPairDStream> {

    @Override
    public JavaPairDStream transform(DriverContext driverContext, Long pid, JavaPairDStream<String, RowWrapper> inStream) {
        SparkDriverContext sparkDriverContext = (SparkDriverContext) driverContext;

        ProcessNode node = (ProcessNode) sparkDriverContext.getProperties().get("_node");

        JsonObject extObject = GsonUtil.getGson().fromJson(node.getConfig().getExt(), JsonObject.class);
        JsonObject defaultObj = extObject.get("default").getAsJsonObject();
        String colName = defaultObj.get("keyFields").getAsString();

        JavaDStream<RowWrapper> dStream = inStream.map(s -> s._2);
        JavaPairDStream<String, RowWrapper> finalDStream = mapToPair(dStream, colName);

        return finalDStream;
    }

    public JavaPairDStream<String, RowWrapper> mapToPair(JavaDStream<RowWrapper> dStream, String colName) {
        JavaPairDStream<String, RowWrapper> finalDStream = null;

        String keyString = colName.substring(0, colName.indexOf(":"));
        String[] keyFields = keyString.split(",");

        finalDStream = dStream.transformToPair(new Function<JavaRDD<RowWrapper>, JavaPairRDD<String, RowWrapper>>() {
            @Override
            public JavaPairRDD<String, RowWrapper> call(JavaRDD<RowWrapper> wrapperMessageJavaRDD) throws Exception {
                JavaRDD<Row> rddRow = wrapperMessageJavaRDD.map(record -> record.getRow());

                JavaPairRDD<String, RowWrapper> pairRDD = rddRow.mapToPair(new PairFunction<Row, String, RowWrapper>() {
                    @Override
                    public Tuple2<String, RowWrapper> call(Row row) throws Exception {
                        String key = "";
                        if (row != null) {
                            for (String keyField : keyFields) {
                                // System.out.println(keyField + " index is " + schema.fieldIndex(keyField));
                                String[] fields = keyField.split("\\.");
                                int i = 0;
                                Row row2 = row;
                                for (i = 0; i < fields.length - 1; i++) {
                                    row2 = row2.getStruct(row2.fieldIndex(fields[i]));
                                }
                                key += row2.getString(row2.fieldIndex(fields[i])) + "#";
                            }
                            key = key.substring(0, key.length() - 1);
                        }
                        return new Tuple2<String, RowWrapper>(key, RowWrapper.convertToWrapperMessage(row));

                    }
                });

                return pairRDD;
            }
        });
        return finalDStream;
    }
}
