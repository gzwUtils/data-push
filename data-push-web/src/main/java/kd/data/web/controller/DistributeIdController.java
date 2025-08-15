package kd.data.web.controller;
import kd.data.service.utils.DistributedIdGenerator;
import kd.data.web.response.ApiResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * 一些组件
 *
 * @author gaozw
 * @date 2025/8/15 14:04
 */
@RestController
@RequestMapping("/common")
public class DistributeIdController {

    @Resource
    private DistributedIdGenerator distributedIdGenerator;

    /**
     * 获取分布式id
     */
    @GetMapping("/getDistributedId")
    public ApiResponse<Long> getDistributedId() {
        long id = distributedIdGenerator.nextId();
        return ApiResponse.success(id);

    }

}
