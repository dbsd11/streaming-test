package group.bison.streaming.core;

import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.core.driver.DriverManager;
import group.bison.streaming.core.process.ProcessNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Created by BSONG on 2019/9/15.
 */
public class StreamingEngine {
    private static final Logger LOG = LogManager.getLogger(StreamingEngine.class);

    public static void start(Long processId, Function fallBack) {
        try {
            //1. 加载process信息
            ProcessNode processNode = new ProcessNode().getById(processId);
            if (processNode.getParentId() != null) {
                //todo　判断是否重新执行父流程
            }

            Map<Long, ProcessNode> parentNodeMap = new HashMap<>();
            getProcessInfo(processNode, parentNodeMap);

            //2. 解析process信息


            //3. 初始化driver
            String driverName = processNode.getConfig().getProperty("driver");
            DriverContext driverContext = DriverManager.create(driverName, processNode.getConfig());

            //4. driver启动
            driverContext.start();

            //5. 监听执行结果,触发异常回调
            new Thread(() -> {
                while (true) {
                    try {
                        int phase = driverContext.getPhase();
                        if (phase == -1) {
                            fallBack.apply(new RuntimeException("处理失败", (Throwable) driverContext.getProperties().get("exception")));
                        }
                    } finally {
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                        }
                    }
                }
            }).start();

        } catch (Exception e) {
            LOG.error("启动引擎失败", e);

            fallBack.apply(new RuntimeException("启动引擎失败", e));
        }
    }

    static void getProcessInfo(ProcessNode processNode, Map<Long, ProcessNode> parentNodeMap) {
        String[] nextProcessNodeIds = StringUtils.isEmpty(processNode.getNextId()) ? new String[0] : processNode.getNextId().split(",");
        for (String nextProcessNode : nextProcessNodeIds) {
            ProcessNode nextNode = processNode.getById(Long.valueOf(nextProcessNode));
            parentNodeMap.put(processNode.getProcessId(), nextNode);
            getProcessInfo(processNode, parentNodeMap);
        }
    }
}
