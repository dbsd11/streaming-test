package group.bison.streaming.spark.schema;

import org.apache.spark.sql.Row;

import java.io.Serializable;

/**
 * Created by BSONG on 2019/10/6.
 */
public class RowWrapper implements Serializable {
    private Row row;

    public RowWrapper(Row row) {
        this.row = row;
    }

    public Row getRow() {
        return row;
    }

    public void setRow(Row row) {
        this.row = row;
    }

    // public WrapperMessage convertRowToWrapperMessage(JavaRDD<Row> )
    public static RowWrapper convertToWrapperMessage(Row record) {
        return new RowWrapper(record);
    }

    //this method converts a WrapperMessage to a Spark SQL Row
    public static Row convertToRow(RowWrapper record) {
        return record.getRow();
    }

    @Override
    public String toString() {
        return "WrapperMessage{" +
                "row=" + row +
                '}';
    }
}
