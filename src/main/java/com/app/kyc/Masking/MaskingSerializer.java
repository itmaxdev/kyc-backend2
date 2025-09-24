package com.app.kyc.Masking;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class MaskingSerializer extends JsonSerializer<Object> {

    private final MaskType maskType;

    public MaskingSerializer(MaskType maskType) {
        this.maskType = maskType;
    }

    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null) {
            gen.writeNull();
            return;
        }

        String strValue = value.toString();  // safe, since we only assign to String fields
        System.out.println("call:::::"+MaskingContext.isMasking());
        if (!MaskingContext.isMasking()) {
        	System.out.println("in unmask:::::");
            gen.writeString(strValue);
            return;
        }else {
        	System.out.println("unmask:::::");
	        switch (maskType) {
	            case NAME:
	                gen.writeString(MaskingUtil.maskName(strValue));
	                break;
	            case PHONE:
	                gen.writeString(MaskingUtil.maskPhone(strValue));
	                break;
	            case IDENTITY:
	                gen.writeString(MaskingUtil.maskIdentity(strValue));
	                break;
	            default:
	                gen.writeString(strValue);
	        }
        }
    }
}
