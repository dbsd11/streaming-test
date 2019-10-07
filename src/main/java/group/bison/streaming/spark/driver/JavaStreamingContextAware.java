package group.bison.streaming.spark.driver;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;

/**
 * Created by BSONG on 2019/10/7.
 */
public interface JavaStreamingContextAware {

    default JavaStreamingContext getStreamingContext(SparkDriverContext sparkDriverContext) {
        JavaStreamingContext javaStreamingContext = null;
        try {
            Field field = ReflectionUtils.findField(SparkDriverContext.class, "streamingContext");
            field.setAccessible(true);
            javaStreamingContext = (JavaStreamingContext) field.get(sparkDriverContext);
        } catch (Exception e) {
            throw new RuntimeException("反射获取streamingContext失败:", e);
        }
        return javaStreamingContext;
    }
}
