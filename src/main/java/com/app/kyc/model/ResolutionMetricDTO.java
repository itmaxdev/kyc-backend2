package com.app.kyc.model;

public class ResolutionMetricDTO {
    private String month;
    private Long resolved;
    private Long unresolved;

    public ResolutionMetricDTO(String month, Long resolved, Long unresolved) {
        this.month = month;
        this.resolved = resolved;
        this.unresolved = unresolved;
    }

    // Getters and setters
    public String getMonth() { return month; }
    public void setMonth(String month) { this.month = month; }

    public Long getResolved() { return resolved; }
    public void setResolved(Long resolved) { this.resolved = resolved; }

    public Long getUnresolved() { return unresolved; }
    public void setUnresolved(Long unresolved) { this.unresolved = unresolved; }
}

