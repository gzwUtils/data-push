package kd.data.web.exception;
import kd.data.service.exception.TaskException;
import kd.data.web.response.ApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import javax.servlet.http.HttpServletRequest;


/**
 * @author gaozw
 * @date 2025/7/25 18:02
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(Exception.class)
    public ApiResponse<Void> handleException(Exception ex, HttpServletRequest request) {
        sendErrorInfo(ex, request);
        return ApiResponse.error("系统错误");
    }

    @ExceptionHandler(TaskException.class)
    public ApiResponse<Void> taskException(TaskException ex, HttpServletRequest request) {
        sendErrorInfo(ex, request);
        return ApiResponse.error("业务异常，请联系管理员"+ex.getMessage());
    }


    private static void sendErrorInfo(Exception ex, HttpServletRequest request) {
        log.error("method : {} error Info {}", request.getMethod(), ex.getMessage(), ex);
    }
}
