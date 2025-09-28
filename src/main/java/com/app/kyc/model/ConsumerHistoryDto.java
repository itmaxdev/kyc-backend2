package com.app.kyc.model;


import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Getter
@Setter
public class ConsumerHistoryDto {
  
    private String status;
    private String note;
    private String inConsistentOn;
    private String consistentOn;
    private String formattedId;

	private String vendorCode;
    
	public ConsumerHistoryDto(String status, String note, String inConsistentOn,String consistentOn,String formattedId,String vendorCode) {
		super();
		this.status = status;
		this.note = note;
		this.inConsistentOn = inConsistentOn;
		this.consistentOn = consistentOn;
		this.formattedId = formattedId;
		this.vendorCode=vendorCode;
	}

}
