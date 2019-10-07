package group.bison.streaming.spark.driver;

import com.google.common.base.Function;
import group.bison.process.core.model.ProcessTopo;
import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.spark.resolver.SparkStreamResolver;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContextState;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Properties;
import java.util.UUID;

/**
 * Created by BSONG on 2019/10/6.
 */
public class SparkDriverContext implements DriverContext {

    private String id;

    private Properties properties;

    private String name;

    private ProcessTopo processTopo;

    private JavaStreamingContext streamingContext;

    public static final String MASTER = "master";

    public static final String CHECKPOINT = "checkpoint";

    public static final String DURATION = "duration";

    public SparkDriverContext(String name, Long processId, Properties properties) {
        this.name = name;
        this.properties = properties;

        //todo 持久化
        this.id = UUID.randomUUID().toString();

        init();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    public String getName() {
        return name;
    }

    public ProcessTopo getProcessTopo() {
        return processTopo;
    }

    @Override
    public void start() {
        streamingContext.start();
    }

    @Override
    public void stop() {
        streamingContext.stop(true, true);
    }

    @Override
    public boolean isRunning() {
        return streamingContext.getState().compareTo(StreamingContextState.ACTIVE) == 0;
    }

    void init() {
        SparkConf conf = new SparkConf().setAppName(name).setMaster(properties.getProperty(MASTER, "local[*]"));
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(Long.valueOf(properties.getProperty(DURATION, "30000"))));

        if (properties.containsKey(CHECKPOINT)) {
            ssc.checkpoint(properties.getProperty(CHECKPOINT));
        }

        //开始解析topo
        resolveProcessTopo();

        this.streamingContext = ssc;
    }

    void resolveProcessTopo() {
        String topoStr = properties.getProperty("processTopo");
        ProcessTopo processTopo = ProcessTopo.getFromBase64Str(topoStr);
        this.processTopo = processTopo;

        SparkStreamResolver sparkStreamResolver = new SparkStreamResolver(this);
        Function<JavaStreamingContext, Void> resolveFunction = sparkStreamResolver.resolve(processTopo);

        resolveFunction.apply(streamingContext);
    }
}
