package group.bison.process.core.model;

import com.google.common.collect.Iterators;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.Base64Utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by BSONG on 2019/10/6.
 */
public class ProcessTopo implements Iterable<ProcessNode>, Serializable {

    private Long id;

    private Long processId;

    private ProcessO process;

    private List<ProcessNode> processNodeList;

    private ProcessNode rootNode;

    private ProcessNode lefNode;

    private VisitInfo visitInfo;

    public Long getId() {
        return id;
    }

    public Long getProcessId() {
        return processId;
    }

    public ProcessO getProcess() {
        return process;
    }

    public ProcessNode getRootNode() {
        return rootNode;
    }

    public ProcessNode getLefNode() {
        return lefNode;
    }

    @Override
    public Iterator<ProcessNode> iterator() {
        List<ProcessNode> processNodes = CollectionUtils.isEmpty(processNodeList) ? Collections.emptyList() : processNodeList;
        Iterator<ProcessNode> iterator = Iterators.transform(processNodes.iterator(), visitInfo::visitNode);

        //生成访问信息记录对象
        this.visitInfo = new VisitInfo();

        return iterator;
    }

    public VisitInfo getVisitInfo() {
        return visitInfo;
    }

    /**
     * 重写toString,统一序列化反序列化
     *
     * @return
     */
    @Override
    public String toString() {
        String base64Str = null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            new ObjectOutputStream(baos).writeObject(this);
            base64Str = new String(Base64Utils.encode(baos.toByteArray()), "utf-8");
        } catch (Exception e) {
            throw new RuntimeException("序列化topo失败", e);
        }

        return base64Str;
    }

    public static ProcessTopo getFromBase64Str(String base64Str) {
        ProcessTopo processTopo = null;
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(base64Str.getBytes("utf-8"));
            Object processTopoObj = new ObjectInputStream(bais).readObject();
            if (processTopoObj != null && processTopoObj instanceof ProcessTopo) {
                processTopo = (ProcessTopo) processTopoObj;
            } else {
                throw new RuntimeException("反序列化结果processTopoObj:" + processTopoObj);
            }
        } catch (Exception e) {
            throw new RuntimeException("反序列化topo失败", e);
        }

        return processTopo;
    }

    static void getProcessInfo(ProcessNode processNode, Map<Long, ProcessNode> parentNodeMap) {
        String[] nextProcessNodeIds = StringUtils.isEmpty(processNode.getNextId()) ? new String[0] : processNode.getNextId().split(",");
        for (String nextProcessNode : nextProcessNodeIds) {
            ProcessNode nextNode = processNode.getById(Long.valueOf(nextProcessNode));
            parentNodeMap.put(processNode.getProcessId(), nextNode);
            getProcessInfo(processNode, parentNodeMap);
        }
    }

    public static class VisitInfo {

        private List<Long> visitedNodeIds = new LinkedList<>();

        private Map<Long, ProcessNode.ProcessNodeConfig> visitedNodeConfig = new MultiValueMap();

        private Long createTime = System.currentTimeMillis();

        private Long updateTime = System.currentTimeMillis();

        ProcessNode visitNode(ProcessNode processNode) {
            visitedNodeIds.add(processNode.getId());
            visitedNodeConfig.put(processNode.getId(), processNode.getConfig());

            updateTime = System.currentTimeMillis();

            //todo 状态同步

            return processNode;
        }

        public Long[] getVisitedNodeIds() {
            return visitedNodeIds.toArray(new Long[0]);
        }

        public Map<Long, ProcessNode.ProcessNodeConfig> getVisitedNodeConfig() {
            return MapUtils.isEmpty(visitedNodeConfig) ? Collections.emptyMap() : new HashMap<>(visitedNodeConfig);
        }

        public Long getCreateTime() {
            return createTime;
        }

        public Long getUpdateTime() {
            return updateTime;
        }
    }
}
