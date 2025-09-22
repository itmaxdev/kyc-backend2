package com.app.kyc.enums;

public enum Lang {
	EN("en"), FR("fr");

	private final String value;

	Lang(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
