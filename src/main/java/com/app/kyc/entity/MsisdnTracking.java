package com.app.kyc.entity;


import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Data
@Getter
@Setter
@Table(name = "msisdn_tracking")
public class MsisdnTracking {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "msisdn", nullable = false, length = 20)
    private String msisdn;

    @Column(name = "first_name", length = 100)
    private String firstName;

    @Column(name = "last_name", length = 100)
    private String lastName;

    @Column(name = "status", length = 20)
    private String status;

    @Column(name = "created_on", nullable = false)
    private LocalDateTime createdOn;
}
