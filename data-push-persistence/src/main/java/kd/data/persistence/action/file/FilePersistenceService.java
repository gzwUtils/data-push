package kd.data.persistence.action.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import kd.data.core.persistence.PersistenceService;
import kd.data.persistence.ProcessModel;
import kd.data.persistence.exception.PersistenceException;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 文件存储服务 - 按日期分片 + 文件滚动
 *
 * @author gaozw
 */
@Slf4j
public class FilePersistenceService implements PersistenceService<ProcessModel> {

    // 配置参数
    private static final long MAX_FILE_SIZE = 100L * 1024 * 1024; // 100MB
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter FILE_TIME_FORMAT = DateTimeFormatter.ofPattern("HHmmss");

    // 运行时状态
    private final Path baseStoragePath;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private BufferedWriter currentWriter;
    private Path currentFilePath;
    private LocalDate currentDate;
    private final AtomicLong currentFileSize = new AtomicLong(0);
    private final ReentrantLock fileLock = new ReentrantLock();

    // 文件序列计数器
    private int fileSequence = 0;

    public FilePersistenceService() {
        this(Paths.get("data/persistence"));
    }

    public FilePersistenceService(Path baseStoragePath) {
        this.baseStoragePath = baseStoragePath;
        ensureDirectoryExists(baseStoragePath);
    }

    @Override
    public void persist(ProcessModel model) {
        try {
            String json = objectMapper.writeValueAsString(model);
            writeToFile(json);
        } catch (IOException e) {
            log.error("文件持久化失败 {}", e.getMessage(), e);
            throw new PersistenceException("文件持久化失败");
        }
    }

    @Override
    public void persistBatch(List<ProcessModel> models) {
        try {
            for (ProcessModel model : models) {
                String json = objectMapper.writeValueAsString(model);
                writeToFile(json);
            }
        } catch (IOException e) {
            log.error("批量文件持久化失败 {}", e.getMessage(), e);
            throw new PersistenceException("批量文件持久化失败");
        }
    }

    private void writeToFile(String json) throws IOException {
        fileLock.lock();
        try {
            // 检查是否需要创建新文件
            if (shouldCreateNewFile()) {
                createNewFile();
            }

            // 写入数据
            String dataLine = json + System.lineSeparator();
            byte[] bytes = dataLine.getBytes(StandardCharsets.UTF_8);

            currentWriter.write(dataLine);
            currentWriter.flush(); // 确保数据立即写入

            // 更新文件大小
            currentFileSize.addAndGet(bytes.length);
        } finally {
            fileLock.unlock();
        }
    }

    private boolean shouldCreateNewFile() {
        // 第一次写入
        if (currentWriter == null) {
            return true;
        }

        // 日期变化
        if (!LocalDate.now().equals(currentDate)) {
            return true;
        }

        // 文件大小超过限制
        return currentFileSize.get() >= MAX_FILE_SIZE;
    }

    private void createNewFile() throws IOException {
        // 关闭当前文件
        closeCurrentFile();

        // 准备新文件路径
        LocalDate today = LocalDate.now();
        String dateDir = today.format(DATE_FORMAT);
        String timestamp = LocalDateTime.now().format(FILE_TIME_FORMAT);

        Path datePath = baseStoragePath.resolve(dateDir);
        ensureDirectoryExists(datePath);

        // 生成新文件名
        String filename = String.format("%s_%s_%03d.log",
                dateDir, timestamp, fileSequence++);

        currentFilePath = datePath.resolve(filename);
        currentDate = today;

        // 创建新文件
        currentWriter = Files.newBufferedWriter(
                currentFilePath,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );

        currentFileSize.set(0);
        log.info("创建新存储文件: {}", currentFilePath);
    }

    private void ensureDirectoryExists(Path path) {
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            log.error("无法创建存储目录: {}", path, e);
            throw new PersistenceException("存储目录创建失败");
        }
    }

    private void closeCurrentFile() {
        if (currentWriter != null) {
            try {
                currentWriter.close();
                log.info("关闭存储文件: {} (大小: {} MB)",
                        currentFilePath,
                        currentFileSize.get() / (1024 * 1024));
            } catch (IOException e) {
                log.error("文件关闭失败: {}", currentFilePath, e);
            } finally {
                currentWriter = null;
            }
        }
    }

    @Override
    public void shutdown() {
        fileLock.lock();
        try {
            closeCurrentFile();
        } finally {
            fileLock.unlock();
        }
    }
}