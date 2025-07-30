package kd.data.core.utils;

import lombok.extern.slf4j.Slf4j;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.util.Collections;
import java.util.List;

/**
 * 压缩工具
 * @author gaozw
 * @date 2025/7/23 18:48
 */
@Slf4j
@SuppressWarnings("unused")
public class CompressionUtil {


    private CompressionUtil(){}

    public static <T> byte[] compressBatch(List<T> batch) {
        if (batch == null || batch.isEmpty()) {
            return new byte[0];
        }
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(batch);
            return Snappy.compress(byteArrayOutputStream.toByteArray());
        } catch (IOException e) {
            log.error("compressBatch error {}",e.getMessage(),e);
        }
        return new byte[0];
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> decompressBatch(byte[] compressed) {
        if (compressed == null || compressed.length == 0) {
            return Collections.emptyList();
        }
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(Snappy.uncompress(compressed));
             ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
            return (List<T>) objectInputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            log.error("decompressBatch error {}",e.getMessage(),e);
        }
        return Collections.emptyList();
    }
}
