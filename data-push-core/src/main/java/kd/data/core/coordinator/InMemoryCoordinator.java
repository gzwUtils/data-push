package kd.data.core.coordinator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 基于内存 用于测试
 *
 * @author gaozw
 * @date 2025/8/27 10:49
 */

public class InMemoryCoordinator  implements DistributedCoordinator {


    private static final String LOCK_PREFIX     = "sync:memory:lock:";
    private static final String CHECKPOINT_PREFIX = "sync:memory:checkpoint:";
    private static final Map<String, String> checkpointStore = new ConcurrentHashMap<>();
    private static final Map<String, LockEntry> lockStore = new ConcurrentHashMap<>();

    private static class LockEntry {
        final ReentrantLock lock = new ReentrantLock();
        final AtomicLong expirationTime = new AtomicLong(0);
        final AtomicReference<Thread> ownerThread = new AtomicReference<>();
    }

    @Override
    public boolean tryLock(String lockKey, int timeoutSeconds) {
        String key = LOCK_PREFIX + lockKey;
        LockEntry entry = lockStore.computeIfAbsent(key, k -> new LockEntry());

        try {
            boolean acquired = entry.lock.tryLock(timeoutSeconds, TimeUnit.SECONDS);
            if (acquired) {
                entry.expirationTime.set(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds));
                entry.ownerThread.set(Thread.currentThread());
            }
            return acquired;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public void unlock(String lockKey) {
        String key = LOCK_PREFIX + lockKey;
        LockEntry entry = lockStore.get(key);
        if (entry != null && entry.lock.isHeldByCurrentThread()) {
            entry.ownerThread.set(null);
            entry.lock.unlock();
        }
    }

    @Override
    public void saveCheckpoint(String key, String checkpoint) {
        checkpointStore.put(CHECKPOINT_PREFIX + key, checkpoint);
    }

    @Override
    public String loadCheckpoint(String key) {
        return checkpointStore.get(CHECKPOINT_PREFIX + key);
    }

    @Override
    public void deleteCheckpoint(String key) {
        checkpointStore.remove(CHECKPOINT_PREFIX + key);
    }

    @Override
    public boolean renewLock(String lockKey, int timeoutSeconds) {
        String key = LOCK_PREFIX + lockKey;
        LockEntry entry = lockStore.get(key);
        if (entry != null && entry.ownerThread.get() == Thread.currentThread()) {
            entry.expirationTime.set(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds));
            return true;
        }
        return false;
    }
}
