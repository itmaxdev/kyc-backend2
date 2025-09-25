package com.app.kyc.entity;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "user_config_settings")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class UserConfig {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "user_id", nullable = false)
    private User user;

    @Column(name = "setting_key", nullable = false, length = 100)
    private String settingKey;   // e.g. "login_minute", "unmask_minute"

    @Column(name = "setting_value", nullable = false)
    private Long settingValue;   // e.g. 5, 2

    @Column(name = "created_on", columnDefinition = "DATETIME DEFAULT CURRENT_TIMESTAMP")
    @Temporal(TemporalType.TIMESTAMP)
    private Date createdOn = new Date();

    @Column(name = "created_by")
    private Long createdBy;
    
    @Column(name = "updated_on", columnDefinition = "DATETIME")
    @Temporal(TemporalType.TIMESTAMP)
    private Date updatedOn = new Date();
    
    @Column(name = "updated_by")
    private Long updatedBy;
}
