package com.app.kyc.Masking;

public class MaskingContext {

    private static final ThreadLocal<Boolean> maskingFlag = ThreadLocal.withInitial(() -> true);

    public static void setMasking(boolean mask) {
        maskingFlag.set(mask);
    }

    public static boolean isMasking() {
        return maskingFlag.get();
    }

    public static void clear() {
        maskingFlag.remove();
    }
}

