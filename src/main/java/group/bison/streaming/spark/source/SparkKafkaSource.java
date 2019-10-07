package group.bison.streaming.spark.source;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import group.bison.process.core.model.ProcessNode;
import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.core.source.StreamSource;
import group.bison.streaming.spark.driver.JavaStreamingContextAware;
import group.bison.streaming.spark.driver.SparkDriverContext;
import group.bison.streaming.spark.utils.GsonUtil;
import kafka.serializer.StringDecoder;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by BSONG on 2019/10/6.
 */
public class SparkKafkaSource implements StreamSource<Void, JavaPairDStream<String, String>>, JavaStreamingContextAware {

    @Override
    public JavaPairDStream<String, String> streamOf(DriverContext driverContext, Long processId, Void schema) {
        SparkDriverContext sparkDriverContext = (SparkDriverContext) driverContext;
        JavaStreamingContext javaStreamingContext = getStreamingContext(sparkDriverContext);

        ProcessNode sourceNode = (ProcessNode) sparkDriverContext.getProperties().get("_source");

        JsonObject extObject = GsonUtil.getGson().fromJson(sourceNode.getConfig().getExt(), JsonObject.class);
        JsonObject kafkaObject = extObject.get("kafka").getAsJsonObject();
        JsonObject clientParamObj = kafkaObject.get("client").getAsJsonObject();
        JsonArray topicsObj = kafkaObject.get("topics").getAsJsonArray();

        Map<String, String> clientParamMap = new HashMap<>();
        Set<String> topics = new HashSet<>();

        clientParamObj.entrySet().forEach(entry -> clientParamMap.put(entry.getKey(), entry.getValue().getAsString()));
        topicsObj.forEach(topic -> topics.add(topic.getAsString()));

        JavaPairDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(javaStreamingContext, String.class, String.class, StringDecoder.class, StringDecoder.class, clientParamMap, topics);
        return kafkaStream;
    }


}
