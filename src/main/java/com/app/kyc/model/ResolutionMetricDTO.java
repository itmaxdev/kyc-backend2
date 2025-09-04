package com.app.kyc.model;

public class ResolutionMetricDTO {
    private String label;
    private Long resolved;
    private Long unresolved;

    public ResolutionMetricDTO(String month, Long resolved, Long unresolved) {
        this.label = month;
        this.resolved = resolved;
        this.unresolved = unresolved;
    }

    // Getters and setters
    public String getLabel() { return label; }
    public void setLabel(String label) { this.label = label; }

    public Long getResolved() { return resolved; }
    public void setResolved(Long resolved) { this.resolved = resolved; }

    public Long getUnresolved() { return unresolved; }
    public void setUnresolved(Long unresolved) { this.unresolved = unresolved; }
}

