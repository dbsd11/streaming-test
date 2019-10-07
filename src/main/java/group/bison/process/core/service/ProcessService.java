package group.bison.process.core.service;

import group.bison.process.core.model.ProcessNode;
import group.bison.process.core.model.ProcessO;

import java.util.List;

/**
 * Created by BSONG on 2019/10/6.
 */
public interface ProcessService {

    public ProcessO getProcessById(Long processId);

    public List<ProcessNode> getProcessNodes(Long processId);
}
