package group.bison.streaming.spark.resolver;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.gson.JsonObject;
import group.bison.process.core.model.ProcessNode;
import group.bison.process.core.model.ProcessTopo;
import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.core.source.StreamSource;
import group.bison.streaming.core.store.StreamStore;
import group.bison.streaming.core.transforms.Transformation;
import group.bison.streaming.spark.transforms.SparkTransformation;
import group.bison.streaming.spark.utils.GsonUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by BSONG on 2019/10/6.
 */
public class SparkStreamResolver {

    private DriverContext driverContext;

    private Map<Long, JavaPairDStream> streamNodeMap = new HashMap<>();

    public SparkStreamResolver(DriverContext driverContext) {
        this.driverContext = driverContext;
    }

    public Function<JavaStreamingContext, Void> resolve(ProcessTopo processTopo) {
        Function<JavaStreamingContext, JavaStreamingContext> inFunction = Functions.identity();

        Function<JavaStreamingContext, JavaPairDStream> parseSourceFunction = parseSourceNode(processTopo.getRootNode());

        Function<JavaPairDStream, JavaPairDStream> parseNodeFunction = Functions.identity();
        Iterator<ProcessNode> processNodeIterator = processTopo.iterator();
        while (processNodeIterator.hasNext()) {
            ProcessNode processNode = processNodeIterator.next();
            if (processNode.getName().equalsIgnoreCase("stream-source") || processNode.getName().equalsIgnoreCase("stream-store")) {
                continue;
            }

            parseNodeFunction = Functions.compose(parseNode(processNode), parseNodeFunction);
        }

        Function<JavaPairDStream, Void> parseStoreFunction = parseStoreNode(processTopo.getLefNode());

        Function<JavaStreamingContext, Void> resolveFunction = Functions.compose(parseStoreFunction, Functions.compose(parseNodeFunction, Functions.compose(parseSourceFunction, inFunction)));
        return resolveFunction;
    }

    Function<JavaPairDStream, JavaPairDStream> parseNode(ProcessNode processNode) {
        return (javaPairDStream) -> {
            ProcessNode.ProcessNodeConfig processNodeConfig = processNode.getConfig();
            String ext = processNodeConfig.getExt();
            JsonObject extObject = GsonUtil.getGson().fromJson(ext, JsonObject.class);

            JavaPairDStream outDStream = null;
            try {
                String transformClsName = extObject.get("transformCls").getAsString();
                Transformation transformation = (Transformation) Class.forName(transformClsName).newInstance();

                driverContext.getProperties().put("_node", processNode);

                outDStream = ((SparkTransformation) transformation).transform(driverContext, processNode.getProcessId(), javaPairDStream);
                streamNodeMap.put(processNode.getId(), outDStream);
            } catch (Exception e) {
                throw new RuntimeException("解析node失败, processNodeId:" + processNode.getId(), e);
            }

            return outDStream;
        };
    }

    Function<JavaStreamingContext, JavaPairDStream> parseSourceNode(ProcessNode processNode) {
        return (streamingContext) -> {
            ProcessNode.ProcessNodeConfig processNodeConfig = processNode.getConfig();
            String ext = processNodeConfig.getExt();
            JsonObject jsonObject = GsonUtil.getGson().fromJson(ext, JsonObject.class);

            JavaPairDStream javaPairDStream = null;
            try {
                String sourceClsName = jsonObject.get("sourceCls").getAsString();
                String sourceSchemaClsName = jsonObject.get("sourceSchemaClsName").getAsString();

                StreamSource streamSource = (StreamSource) Class.forName(sourceClsName).newInstance();
                Object schema = StringUtils.isNotEmpty(sourceSchemaClsName) ? Class.forName(sourceSchemaClsName).newInstance() : null;

                driverContext.getProperties().put("_source", processNode);

                javaPairDStream = (JavaPairDStream) streamSource.streamOf(driverContext, processNode.getProcessId(), schema);
            } catch (Exception e) {
                throw new RuntimeException("解析sourceNode, processNodeId:" + processNode.getId(), e);
            }

            return javaPairDStream;
        };
    }

    Function<JavaPairDStream, Void> parseStoreNode(ProcessNode processNode) {
        return (javaPairDStream) -> {
            ProcessNode.ProcessNodeConfig processNodeConfig = processNode.getConfig();
            String ext = processNodeConfig.getExt();
            JsonObject jsonObject = GsonUtil.getGson().fromJson(ext, JsonObject.class);

            try {
                String storeClsName = jsonObject.get("storeCls").getAsString();
                String storeSchemaClsName = jsonObject.get("storeSchemaClsName").getAsString();

                StreamStore streamStore = (StreamStore) Class.forName(storeClsName).newInstance();
                Object schema = StringUtils.isNotEmpty(storeSchemaClsName) ? Class.forName(storeSchemaClsName).newInstance() : null;

                driverContext.getProperties().put("_store", processNode);
                driverContext.getProperties().put("_javaPairDStream", streamNodeMap.getOrDefault(processNode.getParentId(), javaPairDStream));

                streamStore.persist(driverContext, processNode.getProcessId(), schema);
            } catch (Exception e) {
                throw new RuntimeException("解析storeNode, processNodeId:" + processNode.getId(), e);
            }

            return null;
        };
    }
}
