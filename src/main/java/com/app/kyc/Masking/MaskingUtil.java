package com.app.kyc.Masking;

public class MaskingUtil {

    private MaskingUtil() {} // prevent instantiation

    // Mask String which you pass as string (keep first + rest masked)
    public static String maskName(String str) {
        if (str == null || str.isEmpty()) return str;
        
        return str.charAt(0) + "*".repeat(str.length() - 1);
    }

    // Mask Phone (keep last 4 digits visible)
    public static String maskPhone(String phone) {
    	
    	// Last 4 visible
    	
        /*if (phone == null || phone.length() < 4) return phone;
        int visibleDigits = 4;
        return "*".repeat(phone.length() - visibleDigits) + phone.substring(phone.length() - visibleDigits);*/
    	
    	if (phone == null || phone.length() <= 4) {
            return phone; 
        }
        int visibleDigits = phone.length() - 4; 
        String prefix = phone.substring(0, visibleDigits);
        return prefix + "****";
    }

    // Mask Identity 
    public static String maskIdentity(String id) {
        if (id == null || id.length() < 4) return id;
        int visible = 4;
       //It will keep last 4 , rest masked
        return "*".repeat(id.length() - visible) + id.substring(id.length() - visible);
        
        //It will show first 2 + last 2, rest masked
        /*String start = id.substring(0, visible);
        String end = id.substring(id.length() - visible);
        return start + "*".repeat(id.length() - (visible * 2)) + end;*/
    }
}
