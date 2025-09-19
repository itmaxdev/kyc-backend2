package com.app.kyc.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;


@Entity
@Data
@Setter
@Getter
@Table( name = "consumer_tracking")
public class ConsumerTracking {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "consistent_on")
    private String consistentOn;

    @ManyToOne
    private ServiceProvider serviceProvider;
    
    private Long consumerId;
    
    private Boolean isConsistent;

	public ConsumerTracking(Long consumerId, ServiceProvider serviceProvider, String consistentOn, Boolean isConsistent) {
		super();
		this.consistentOn = consistentOn;
		this.serviceProvider = serviceProvider;
		this.consumerId = consumerId;
		this.isConsistent = isConsistent;
	}

}
