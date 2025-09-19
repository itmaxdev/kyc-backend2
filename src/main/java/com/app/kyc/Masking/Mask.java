package com.app.kyc.Masking;
import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)   // available at runtime
@Target(ElementType.FIELD)
public @interface Mask {
	 MaskType value();
}
