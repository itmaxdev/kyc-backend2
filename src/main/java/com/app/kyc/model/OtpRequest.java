package com.app.kyc.model;

import com.app.kyc.enums.Channel;
import com.app.kyc.enums.Lang;
import com.app.kyc.enums.OtpPurpose;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OtpRequest {
	private Channel channel; 
	private OtpPurpose purpose;
	private Lang lang;
	private String email;
	private Long userId;
}
