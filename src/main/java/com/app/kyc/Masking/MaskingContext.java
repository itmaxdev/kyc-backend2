package com.app.kyc.Masking;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MaskingContext {

    private static final ThreadLocal<Boolean> maskingFlag = ThreadLocal.withInitial(() -> true);
    private static final Map<Long, Long> tempUnmaskExpiry = new ConcurrentHashMap<>();

    public static void setMasking(boolean mask) {
        maskingFlag.set(mask);
    }

    public static boolean isMasking() {
        return maskingFlag.get();
    }

    public static void clear() {
        maskingFlag.remove();
    }
    
    // --- Temporary unmask logic ---
    public static void setTemporaryUnmask(Long userId, long durationMillis) {
        tempUnmaskExpiry.put(userId, System.currentTimeMillis() + durationMillis);
    }

    public static boolean isTemporarilyUnmasked(Long userId) {
        Long expiry = tempUnmaskExpiry.get(userId);
        if (expiry == null) return false;
        if (System.currentTimeMillis() > expiry) {
            tempUnmaskExpiry.remove(userId); // cleanup expired
            return false;
        }
        return true;
    }
}

