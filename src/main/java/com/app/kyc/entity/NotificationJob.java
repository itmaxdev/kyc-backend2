package com.app.kyc.entity;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import com.app.kyc.model.AnomalyStatus;

@Entity
@Table(name = "notification_jobs")
public class NotificationJob {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	private Long anomalyId;

	@Enumerated(EnumType.STRING)
	private AnomalyStatus status;

	private String state; // PENDING, COMPLETED, FAILED

	private String errorMessage;

	@Temporal(TemporalType.TIMESTAMP)
	private Date createdAt;

	@Temporal(TemporalType.TIMESTAMP)
	private Date updatedAt;

	public NotificationJob() {
	}

	public NotificationJob(Long anomalyId, AnomalyStatus status) {
		this.anomalyId = anomalyId;
		this.status = status;
		this.state = "PENDING";
		this.createdAt = new Date();
		this.updatedAt = new Date();
	}

	// --- Getters / Setters ---

	public void markCompleted() {
		this.state = "COMPLETED";
		this.updatedAt = new Date();
	}

	public void markFailed(String error) {
		this.state = "FAILED";
		this.errorMessage = error;
		this.updatedAt = new Date();
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getAnomalyId() {
		return anomalyId;
	}

	public void setAnomalyId(Long anomalyId) {
		this.anomalyId = anomalyId;
	}

	public AnomalyStatus getStatus() {
		return status;
	}

	public void setStatus(AnomalyStatus status) {
		this.status = status;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public String getErrorMessage() {
		return errorMessage;
	}
}
