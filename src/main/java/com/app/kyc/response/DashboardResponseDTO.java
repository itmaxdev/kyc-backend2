package com.app.kyc.response;

import com.app.kyc.model.DashboardObjectInterface;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DashboardResponseDTO {

   private List<DashboardObjectInterface> listIndustry;
   private List<DashboardObjectInterface> listServiceProvider;
   private List<DashboardObjectInterface> listServiceType;

   private Integer numServiceProviders;
   private Integer numConsistentConsumers;
   private Integer numConsumers;
   private Integer numNonConsistentConsumers;
   private Integer numSubscriptions;
   private Integer numAnomaliesReported;
   private Integer numAnomaliesInProgress;
   private Integer numAnomaliesResolved;
   private Integer numAnomaliesWithdrawn;

   private Double numAverageResolutionTime;

   private List<DashboardObjectInterface> consumers;
   private List<DashboardObjectInterface> subscriptions;
   private List<DashboardObjectInterface> anomaliesByStatus;
   private List<DashboardObjectInterface> timeConsumers;
   private List<DashboardObjectInterface> timeFixedAnomalies;
   private List<DashboardObjectInterface> timeSubscriptions;
   private List<DashboardObjectInterface> color;
   private List<DashboardObjectInterface> anomalies;
   private List<DashboardObjectInterface> anomalyTypes;
   private List<DashboardObjectInterface> resolutionMetrics;
   private List<DashboardObjectInterface> consumersByConsistency;
}
