package com.app.kyc.model;

public class ConsumerHistoryDto {
  
    private String status;
    private String note;
    private String inConsistentOn;
    private String consistentOn;
    private String formattedId;
    
	public ConsumerHistoryDto(String status, String note, String inConsistentOn,String consistentOn,String formattedId) {
		super();
		this.status = status;
		this.note = note;
		this.inConsistentOn = inConsistentOn;
		this.consistentOn = consistentOn;
		this.formattedId = formattedId;
	}
	
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getNote() {
		return note;
	}
	public void setNote(String note) {
		this.note = note;
	}
	public String getConsistentOn() {
		return consistentOn;
	}
	public void setConsistentOn(String consistentOn) {
		this.consistentOn = consistentOn;
	}
	public String getInConsistentOn() {
		return inConsistentOn;
	}
	public void setInConsistentOn(String inConsistentOn) {
		this.inConsistentOn = inConsistentOn;
	}

	public String getFormattedId() {
		return formattedId;
	}

	public void setFormattedId(String formattedId) {
		this.formattedId = formattedId;
	}
}
