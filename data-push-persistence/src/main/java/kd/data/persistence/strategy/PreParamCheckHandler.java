package kd.data.persistence.strategy;

import kd.data.persistence.KdProcess;
import kd.data.persistence.ProcessContext;
import kd.data.persistence.ProcessModel;
import kd.data.persistence.response.R;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * 参数校验
 * @author gaozw
 * @date 2025/8/8 16:17
 */
@SuppressWarnings("unused")
public class PreParamCheckHandler implements KdProcess<ProcessModel> {
    @Override
    public void process(ProcessContext<ProcessModel> content) {
        ProcessModel model = content.getModel();
        String taskId = model.getTaskId();
        if(StringUtils.isEmpty(taskId)){
            content.setNeedBreak(true).setResponse(R.fail("taskId is empty "));
            return;
        }
        Map<String, Object> sourceConfig = model.getSourceConfig();
        if(sourceConfig.isEmpty()){
            content.setNeedBreak(true).setResponse(R.fail("sourceConfig is empty"));
            return;
        }
        Map<String, Object> desConfig = model.getDesConfig();
        if(desConfig.isEmpty()){
            content.setNeedBreak(true).setResponse(R.fail("desConfig is empty"));
            return;
        }

        String sourceType = model.getSourceType();
        if(StringUtils.isEmpty(sourceType)){
            content.setNeedBreak(true).setResponse(R.fail("sourceType is empty"));
        }
    }
}
