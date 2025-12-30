package kd.data.persistence.strategy;

import kd.data.core.model.SyncStats;
import kd.data.persistence.KdProcess;
import kd.data.persistence.ProcessContext;
import kd.data.persistence.ProcessModel;
import kd.data.persistence.response.R;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * 参数校验（智能识别配置和统计）
 */
public class PreParamCheckHandler implements KdProcess<ProcessModel> {

    @Override
    public void process(ProcessContext<ProcessModel> content) {
        ProcessModel model = content.getModel();
        String taskId = model.getTaskId();

        if (StringUtils.isEmpty(taskId)) {
            content.setNeedBreak(true).setResponse(R.fail("taskId is empty "));
            return;
        }

        // 判断是配置还是统计
        SyncStats stats = model.getSyncStats();

        if (stats != null) {
            // 统计模式
            validateStats(taskId, stats);
        } else {
            // 配置模式
            validateConfig(model, content);
        }
    }

    private void validateConfig(ProcessModel model, ProcessContext<ProcessModel> content) {
        Map<String, Object> sourceConfig = model.getSourceConfig();
        if (sourceConfig == null || sourceConfig.isEmpty()) {
            content.setNeedBreak(true).setResponse(R.fail("sourceConfig is empty"));
            return;
        }

        Map<String, Object> desConfig = model.getDesConfig();
        if (desConfig == null || desConfig.isEmpty()) {
            content.setNeedBreak(true).setResponse(R.fail("desConfig is empty"));
            return;
        }

        String sourceType = model.getSourceType();
        if (StringUtils.isEmpty(sourceType)) {
            content.setNeedBreak(true).setResponse(R.fail("sourceType is empty"));
        }
    }

    private void validateStats(String taskId, SyncStats stats) {
        if (StringUtils.isEmpty(stats.getTaskId())) {
            stats.setTaskId(taskId);
        }
    }
}