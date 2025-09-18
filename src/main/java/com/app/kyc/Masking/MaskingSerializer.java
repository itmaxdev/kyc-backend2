package com.app.kyc.Masking;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

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
