package kd.data.persistence.action.file;
import com.fasterxml.jackson.databind.ObjectMapper;
import kd.data.core.persistence.PersistenceService;
import kd.data.persistence.ProcessModel;
import kd.data.persistence.exception.PersistenceException;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * jdbc
 *
 * @author gaozw
 * @date 2025/8/11 17:11
 */
@SuppressWarnings("unused")
@Slf4j
public class FilePersistenceService implements PersistenceService<ProcessModel> {


    private final Path storagePath;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public FilePersistenceService() {
        this(Paths.get("data/persistence"));
    }

    public FilePersistenceService(Path storagePath) {
        this.storagePath = storagePath;

        try {
            Files.createDirectories(storagePath);
        } catch (IOException e) {
            log.error("无法创建存储目录", e);
        }
    }
    @Override
    public void persist(ProcessModel model) {
        Path filePath = storagePath.resolve(model.getTaskId() + ".json");

        try {
            String json = objectMapper.writeValueAsString(model);
            Files.write(filePath, json.getBytes(StandardCharsets.UTF_8));
            log.info("文件持久化成功: {}", filePath);
        } catch (IOException e) {
            log.error("文件持久化失败 {}", e.getMessage(),e);
            throw new PersistenceException("文件持久化失败");
        }
    }

    @Override
    public void persistBatch(List<ProcessModel> models) {
        for (ProcessModel model : models) {
            persist(model);
        }
    }

    @Override
    public void shutdown() {
        //无需特殊关闭
    }
}
