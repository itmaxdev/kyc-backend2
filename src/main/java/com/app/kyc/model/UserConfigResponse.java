package com.app.kyc.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class UserConfigResponse {
	private Long loginMinute;
	private Long unmaskMinute;

	public UserConfigResponse(Long loginMinute, Long unmaskMinute) {
		this.loginMinute = loginMinute;
		this.unmaskMinute = unmaskMinute;
	}
}
