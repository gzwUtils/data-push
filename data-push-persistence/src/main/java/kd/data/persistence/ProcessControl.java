package kd.data.persistence;
import kd.data.persistence.exception.PersistenceException;
import kd.data.persistence.response.R;
import kd.data.persistence.template.PersistenceTemplate;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 执行控制器
 * @author gaozw
 * @date 2025/8/8 15:09
 */

@Data
@SuppressWarnings("all")
@Slf4j
public class ProcessControl {


    /**
     * 模板映射
     */
    private  Map<String, PersistenceTemplate> templateConfig = null;


    /**
     * 执行责任链
     *
     * @param context
     * @return 返回上下文内容
     */
    public ProcessContext process(ProcessContext context) {

        /**
         * 前置检查
         */
        try {
            preCheck(context);
        } catch (PersistenceException e) {
            log.error("执行 process error {}",e.getMessage(),e);
        }

        /**
         * 遍历流程节点
         */
        List<KdProcess> processList = templateConfig.get(context.getStrategyCode()).getKdProcessList();
        for (KdProcess businessProcess : processList) {
            businessProcess.process(context);
            if (context.getNeedBreak()) {
                break;
            }
        }
        return context;
    }


    /**
     * 执行前检查，出错则抛出异常
     *
     * @param context 执行上下文
     * @throws ProcessException 异常信息
     */
    private void preCheck(ProcessContext context) throws PersistenceException {
        // 上下文
        if (Objects.isNull(context)) {
            context = new ProcessContext();
            context.setResponse(R.fail("process context is null ")).setNeedBreak(true);
            throw new PersistenceException("process context is null ");
        }

        // 业务代码
        String businessCode = context.getStrategyCode();
        if (StringUtils.isBlank(businessCode)) {
            context.setResponse(R.fail("strategy code is null ")).setNeedBreak(true);
            throw new PersistenceException("strategy code is null ");
        }

        // 执行模板
        PersistenceTemplate processTemplate = templateConfig.get(businessCode);
        if (Objects.isNull(processTemplate)) {
            context.setResponse(R.fail("process template is null")).setNeedBreak(true);
            throw new PersistenceException("process template is null");
        }

        // 执行模板列表
        List<KdProcess> processList = processTemplate.getKdProcessList();
        if (processList.isEmpty()) {
            context.setResponse(R.fail("template list is  null ")).setNeedBreak(true);
            throw new PersistenceException("template list is  null ");
        }

    }


}
