package com.app.kyc.model;

public class AnomalyTypeDto {
    private String name;
    private String severity; // <-- mapped meaning like Serious / Critical

    public AnomalyTypeDto(String name, String severity) {
        this.name = name;
        this.severity = severity;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }
}