package com.app.kyc.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Filter {
    private Boolean consistent;

    @JsonAlias({"serviceProviderID","serviceProviderId"})
    @JsonProperty("serviceProviderID")
    private Long serviceProviderID;

    private Boolean isResolved;

    // NEW: "ALL" | "CONSISTENT" | "INCONSISTENT"
    @JsonAlias({"type"})
    private String type;
    
    @JsonAlias({"anomalyStatus"})
    private AnomalyStatus anomalyStatus;
    
    @JsonAlias({"anomalyType"})
    private Long anomalyType;
    
    @JsonAlias({"resolution"})
    private String resolution;

    public Boolean getConsistent() { return consistent; }
    public void setConsistent(Boolean consistent) { this.consistent = consistent; }

    public Long getServiceProviderID() { return serviceProviderID; }
    public void setServiceProviderID(Long serviceProviderID) { this.serviceProviderID = serviceProviderID; }

    public Boolean getIsResolved() { return isResolved; }
    public void setIsResolved(Boolean isResolved) { this.isResolved = isResolved; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    
	public AnomalyStatus getAnomalyStatus() {return anomalyStatus;}
	public void setAnomalyStatus(AnomalyStatus anomalyStatus) {this.anomalyStatus = anomalyStatus;}
	
	public Long getAnomalyType() {return anomalyType;}
	public void setAnomalyType(Long anomalyType) {this.anomalyType = anomalyType;}
	
	public String getResolution() {return resolution;}
	public void setResolution(String resolution) {this.resolution = resolution;}
}
