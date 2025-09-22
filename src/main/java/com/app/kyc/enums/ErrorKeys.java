package com.app.kyc.enums;

public enum ErrorKeys {
	NOT_FOUND("Resource not found"), 
	USER_NOT_FOUND("User not found");

	private final String value;

	ErrorKeys(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
