package com.app.kyc.Masking;

import java.util.List;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;

public class MaskingBeanSerializerModifier extends BeanSerializerModifier {
    @Override
    public List<BeanPropertyWriter> changeProperties(SerializationConfig config,
                                                     BeanDescription beanDesc,
                                                     List<BeanPropertyWriter> beanProperties) {

        for (BeanPropertyWriter writer : beanProperties) {
            Mask annotation = writer.getAnnotation(Mask.class);
            if (annotation != null && writer.getType().getRawClass() == String.class) {
                writer.assignSerializer(new MaskingSerializer(annotation.value()));
            }
        }
        return beanProperties;
    }
}
