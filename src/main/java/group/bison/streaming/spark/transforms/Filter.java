package group.bison.streaming.spark.transforms;

import com.google.gson.JsonObject;
import group.bison.process.core.model.ProcessNode;
import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.spark.driver.SparkDriverContext;
import group.bison.streaming.spark.schema.RowWrapper;
import group.bison.streaming.spark.utils.GsonUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.util.Date;

/**
 * Created by cloudera on 5/21/17.
 */
public class Filter implements SparkTransformation {

    @Override
    public JavaPairDStream transform(DriverContext driverContext, Long pid, JavaPairDStream<String, RowWrapper> inStream) {
        SparkDriverContext sparkDriverContext = (SparkDriverContext) driverContext;

        ProcessNode node = (ProcessNode) sparkDriverContext.getProperties().get("_node");

        JsonObject extObject = GsonUtil.getGson().fromJson(node.getConfig().getExt(), JsonObject.class);
        JsonObject filterObj = extObject.get("filter").getAsJsonObject();
        int count = filterObj.get("count").getAsInt();
        String schema = filterObj.get("schema").getAsString();
        StructType structType = (StructType) StructType.fromJson(schema);

        JavaPairDStream<String, RowWrapper> filteredDstream = inStream.transformToPair(new Function<JavaPairRDD<String, RowWrapper>, JavaPairRDD<String, RowWrapper>>() {
            @Override
            public JavaPairRDD<String, RowWrapper> call(JavaPairRDD<String, RowWrapper> rddPairWrapperMessage) throws Exception {
                System.out.println("beginning of filter/validation = " + new Date().getTime() + "for pid = " + pid);
                JavaRDD<Row> rddRow = rddPairWrapperMessage.map(s -> s._2.getRow());
                SQLContext sqlContext = SQLContext.getOrCreate(rddRow.context());
                DataFrame dataFrame = sqlContext.createDataFrame(rddRow, structType);
                DataFrame filteredDF = null;

                if (dataFrame != null) {
                    System.out.println("showing dataframe before filter ");
                    dataFrame.show(100);
                    Column sqlDataFrame = null;
                    for (int i = 1; i <= count; i++) {
                        String logicalOperator = filterObj.get("logicalOperator_" + i).getAsString();
                        String check = filterObj.get("operator_" + i).getAsString();
                        String colNameProperty = filterObj.get("column_" + i).getAsString();
                        String colName = colNameProperty.substring(0, colNameProperty.indexOf(":"));
                        String filterValue = filterObj.get("filterValue_" + i).getAsString();
                        System.out.println("logicalOperator = " + logicalOperator);
                        System.out.println("operator = " + check);
                        System.out.println("filtervalue = " + filterValue);
                        System.out.println("colName = " + colName);


                        switch (logicalOperator) {
                            case "NONE":
                                switch (check) {
                                    case "equals":
                                        sqlDataFrame = dataFrame.col(colName).equalTo(filterValue);
                                        break;
                                    case "begins with":
                                        sqlDataFrame = sqlDataFrame.and(dataFrame.col(colName).startsWith(filterValue));
                                        break;
                                    case "ends with":
                                        sqlDataFrame = sqlDataFrame.and(dataFrame.col(colName).endsWith(filterValue));
                                        break;
                                    case "is null":
                                        sqlDataFrame = dataFrame.col(colName).isNull();
                                        break;
                                    case "is not null":
                                        sqlDataFrame = dataFrame.col(colName).notEqual("");
                                        break;
                                    case "greater than":
                                        sqlDataFrame = dataFrame.col(colName).gt(Long.valueOf(filterValue));
                                        break;
                                }
                                break;


                            case "AND":
                                switch (check) {
                                    case "equals":
                                        sqlDataFrame = sqlDataFrame.and(dataFrame.col(colName).equalTo(filterValue));
                                        break;
                                    case "begins with":
                                        sqlDataFrame = sqlDataFrame.and(dataFrame.col(colName).startsWith(filterValue));
                                        break;
                                    case "ends with":
                                        sqlDataFrame = sqlDataFrame.and(dataFrame.col(colName).endsWith(filterValue));
                                        break;
                                    case "is null":
                                        sqlDataFrame = sqlDataFrame.and(dataFrame.col(colName).isNull());
                                        break;
                                    case "is not null":
                                        sqlDataFrame = sqlDataFrame.and(dataFrame.col(colName).isNotNull());
                                        break;
                                    case "greater than":
                                        sqlDataFrame = sqlDataFrame.and(dataFrame.col(colName).gt(filterValue));
                                        break;
                                }
                                break;


                            case "OR":
                                switch (check) {
                                    case "equals":
                                        sqlDataFrame = sqlDataFrame.or(dataFrame.col(colName).equalTo(filterValue));
                                        break;
                                    case "begins with":
                                        sqlDataFrame = sqlDataFrame.and(dataFrame.col(colName).startsWith(filterValue));
                                        break;
                                    case "ends with":
                                        sqlDataFrame = sqlDataFrame.and(dataFrame.col(colName).endsWith(filterValue));
                                        break;
                                    case "is null":
                                        sqlDataFrame = sqlDataFrame.or(dataFrame.col(colName).isNull());
                                        break;
                                    case "is not null":
                                        sqlDataFrame = sqlDataFrame.or(dataFrame.col(colName).isNotNull());
                                        break;
                                    case "greater than":
                                        sqlDataFrame = sqlDataFrame.or(dataFrame.col(colName).gt(filterValue));
                                        break;
                                }
                                break;
                        }
                    }

                    filteredDF = dataFrame.filter(sqlDataFrame);
                    System.out.println("showing dataframe after filter ");
                    filteredDF.show(100);
                }

                JavaPairRDD<String, RowWrapper> finalRDD = null;
                if (filteredDF != null)
                    finalRDD = filteredDF.javaRDD().mapToPair(s -> new Tuple2<String, RowWrapper>(null, new RowWrapper(s)));
                System.out.println("End of filter/validation = " + new Date().getTime() + "for pid = " + pid);
                return finalRDD;
            }
        });

        return filteredDstream;
    }
}