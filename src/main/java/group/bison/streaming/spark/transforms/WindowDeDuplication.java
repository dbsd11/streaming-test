package group.bison.streaming.spark.transforms;

import com.google.common.base.Optional;
import group.bison.streaming.spark.schema.RowWrapper;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

/**
 * Created by cloudera on 8/3/17.
 */
public class WindowDeDuplication {

    public JavaPairDStream<String, RowWrapper> convertJavaPairDstream(JavaPairDStream<String, RowWrapper> inputDstream, long duration) {
        JavaPairDStream<String, Row> idValueStream = inputDstream.mapValues(s -> s.getRow());
        JavaMapWithStateDStream<String, Row, String, Row> mappedStream = idValueStream.mapWithState(StateSpec.function(new DuplicateChecker()).timeout(new Duration(duration)));
        // Start the computation
        JavaPairDStream<String, RowWrapper> deduplicatedStream = mappedStream.mapToPair(s -> new Tuple2<String, RowWrapper>(null, new RowWrapper(s)));
        return deduplicatedStream;

    }

}

class DuplicateChecker implements Function4<Time, String, Optional<Row>, State<String>, Optional<Row>> {

    @Override
    public Optional<Row> call(Time time, String key, Optional<Row> value, State<String> state) throws Exception {
        String existingState = (state.exists() ? state.get() : new String());
        System.out.println("key = " + key);
        if (existingState.equals(key)) {
            System.out.println(" Duplicate found");
            return Optional.absent();
        } else {
            System.out.println(" New Record found");
            state.update(key);
            return value;
        }
    }
}
