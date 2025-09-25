package com.app.kyc.model;

import com.app.kyc.entity.Otp;
import com.app.kyc.entity.User;

public class OtpMapper {
	 public static Otp toEntity(OtpRequest otpRequest, User user, String hashedOtp) {
	        Otp otp = new Otp();
	        otp.setUser(user);
	        otp.setPurpose(otpRequest.getPurpose());
	        otp.setChannel(otpRequest.getChannel());
	        return otp;
	    }
}
