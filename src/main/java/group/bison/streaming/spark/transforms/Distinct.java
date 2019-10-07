package group.bison.streaming.spark.transforms;

import com.google.gson.JsonObject;
import group.bison.process.core.model.ProcessNode;
import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.spark.driver.SparkDriverContext;
import group.bison.streaming.spark.schema.RowWrapper;
import group.bison.streaming.spark.utils.GsonUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * Created by cloudera on 6/8/17.
 */
public class Distinct implements SparkTransformation {

    @Override
    public JavaPairDStream transform(DriverContext driverContext, Long pid, JavaPairDStream<String, RowWrapper> inStream) {
        SparkDriverContext sparkDriverContext = (SparkDriverContext) driverContext;
        JavaStreamingContext jssc = getStreamingContext(sparkDriverContext);

        ProcessNode node = (ProcessNode) sparkDriverContext.getProperties().get("_node");

        JsonObject extObject = GsonUtil.getGson().fromJson(node.getConfig().getExt(), JsonObject.class);
        JsonObject defaultObj = extObject.get("default").getAsJsonObject();
        String schema = defaultObj.get("schema").getAsString();
        StructType structType = (StructType) StructType.fromJson(schema);

        JavaDStream<RowWrapper> dStream = inStream.map(s -> s._2);
        JavaDStream<RowWrapper> finalDStream = dStream.transform(new Function<JavaRDD<RowWrapper>, JavaRDD<RowWrapper>>() {
            @Override
            public JavaRDD<RowWrapper> call(JavaRDD<RowWrapper> rddWrapperMessage) throws Exception {

                JavaRDD<Row> rddRow = rddWrapperMessage.map(new Function<RowWrapper, Row>() {
                                                                @Override
                                                                public Row call(RowWrapper wrapperMessage) throws Exception {
                                                                    return wrapperMessage.getRow();
                                                                }
                                                            }
                );


                SQLContext sqlContext = SQLContext.getOrCreate(rddWrapperMessage.context());
                DataFrame dataFrame = sqlContext.createDataFrame(rddRow, structType);
                DataFrame filteredDF = null;

                if (dataFrame != null && !dataFrame.rdd().isEmpty()) {
                    System.out.println("showing dataframe before distinct ");
                    dataFrame.show(100);
                    filteredDF = dataFrame.distinct();
                    filteredDF.show(100);
                    System.out.println("showing dataframe after distinct ");
                }

                JavaRDD<RowWrapper> finalRDD = jssc.sc().emptyRDD();
                if (filteredDF != null) {
                    finalRDD = filteredDF.javaRDD().map(new Function<Row, RowWrapper>() {
                                                            @Override
                                                            public RowWrapper call(Row row) throws Exception {
                                                                return new RowWrapper(row);
                                                            }
                                                        }
                    );

                }
                return finalRDD;
                // return filteredDF.javaRDD();
            }
        });
        return finalDStream.mapToPair(s -> new Tuple2<String, RowWrapper>(null, s));
    }
}
