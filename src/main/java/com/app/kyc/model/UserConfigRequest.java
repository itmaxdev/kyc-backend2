package com.app.kyc.model;

import java.util.Map;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class UserConfigRequest {
	@NotNull(message = "userId is required")
	private Long userId;
	@NotNull(message = "settings are required")
	@NotEmpty(message = "settings cannot be empty")
	private Map<String, Long> settings; // key-value pairs
}
