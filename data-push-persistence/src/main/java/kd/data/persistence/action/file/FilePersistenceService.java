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
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class FilePersistenceService implements PersistenceService<ProcessModel> {

    private static final long MAX_FILE_SIZE = 100L * 1024 * 1024; // 100MB
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    // 文件类型后缀映射
    private static final String CONFIG_SUFFIX = "0"; // 配置文件后缀
    private static final String STATS_SUFFIX = "1";  // 统计文件后缀

    private final Path baseStoragePath;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // 为每种类型维护独立的写入上下文
    private final ConcurrentMap<String, FileWriterContext> writerContexts = new ConcurrentHashMap<>();

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
            String fileSuffix = determineFileSuffix(model); // 根据模型类型决定文件后缀
            writeToFile(fileSuffix, json);
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
                String fileSuffix = determineFileSuffix(model);
                writeToFile(fileSuffix, json);
            }
        } catch (IOException e) {
            log.error("批量文件持久化失败 {}", e.getMessage(), e);
            throw new PersistenceException("批量文件持久化失败");
        }
    }

    /**
     * 根据模型类型决定文件后缀
     * 0: 配置类型 (CONFIG)
     * 1: 统计类型 (STATS)
     */
    private String determineFileSuffix(ProcessModel model) {
        // 根据syncStats判断类型
        return (model.getSyncStats() != null) ? STATS_SUFFIX : CONFIG_SUFFIX;
    }

    private void writeToFile(String fileSuffix, String json) throws IOException {
        FileWriterContext context = writerContexts.computeIfAbsent(fileSuffix,
                key -> new FileWriterContext(baseStoragePath, key));

        context.lock();
        try {
            if (context.shouldCreateNewFile()) {
                context.createNewFile();
            }

            String dataLine = json + System.lineSeparator();
            context.writer.write(dataLine);
            context.writer.flush();
            context.currentFileSize.addAndGet(dataLine.getBytes(StandardCharsets.UTF_8).length);
        } finally {
            context.unlock();
        }
    }

    private void ensureDirectoryExists(Path path) {
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            log.error("无法创建存储目录: {}", path, e);
            throw new PersistenceException("存储目录创建失败");
        }
    }

    @Override
    public void shutdown() {
        writerContexts.values().forEach(FileWriterContext::close);
    }

    /**
     * 文件写入上下文
     */
    private static class FileWriterContext {
        private final Path basePath;
        private final String fileSuffix; // 文件后缀: "0" 或 "1"
        private final ReentrantLock lock = new ReentrantLock();
        private BufferedWriter writer;
        private Path currentFilePath;
        private LocalDate currentDate;
        private final AtomicLong currentFileSize = new AtomicLong(0);
        private int fileSequence = 0;

        FileWriterContext(Path basePath, String fileSuffix) {
            this.basePath = basePath;
            this.fileSuffix = fileSuffix;
        }

        void lock() { lock.lock(); }
        void unlock() { lock.unlock(); }

        boolean shouldCreateNewFile() {
            if (writer == null) return true;
            if (!LocalDate.now().equals(currentDate)) return true;
            return currentFileSize.get() >= MAX_FILE_SIZE;
        }

        void createNewFile() throws IOException {
            close();

            LocalDate today = LocalDate.now();
            String dateDir = today.format(DATE_FORMAT);
            Path datePath = basePath.resolve(dateDir);
            Files.createDirectories(datePath);

            // 生成文件名: yyyyMMdd_xxx.后缀
            String filename = String.format("%s_%03d.%s",
                    dateDir, fileSequence++, fileSuffix);
            currentFilePath = datePath.resolve(filename);
            currentDate = today;

            writer = Files.newBufferedWriter(
                    currentFilePath,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );

            currentFileSize.set(0);

            // 日志中显示文件类型信息
            String fileType = CONFIG_SUFFIX.equals(fileSuffix) ? "config" : "stats";
            log.info("创建新文件: {} (类型: {})",
                    currentFilePath, fileType);
        }

        void close() {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    log.error("文件关闭失败: {}", currentFilePath, e);
                }
                writer = null;
            }
        }
    }
}