package com.app.kyc.response;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

import com.app.kyc.model.AnomalyTrackingDto;
import com.app.kyc.model.AnomlyDto;

public class AnomalyDetailsResponseDTO {

   private AnomlyDto anomalyDto;
   private List<AnomalyTrackingDto> anomalyTrackingDto;

   private long consistentCount;       // resolved_count
   private long inconsistentCount;
   //private long totalCount;            // consistent + inconsistent

   // NOTE: this is the resolved percentage now (kept the old name to avoid breaking callers)
   private double partiallyResolvedPercentage;

   public AnomalyDetailsResponseDTO(
           AnomlyDto anomalyDto,
           List<AnomalyTrackingDto> anomalyTrackingDto,
           int consistentCount,
           int inconsistentCount
   ) {
      this(anomalyDto, anomalyTrackingDto, (long) consistentCount, (long) inconsistentCount);
   }

   public AnomalyDetailsResponseDTO(
           AnomlyDto anomalyDto,
           List<AnomalyTrackingDto> anomalyTrackingDto,
           long consistentCount,
           long inconsistentCount
   ) {
      this.anomalyDto = anomalyDto;
      this.anomalyTrackingDto = anomalyTrackingDto;
      this.consistentCount = consistentCount;
      this.inconsistentCount = inconsistentCount;
   }

   // Optional: constructor that accepts a precomputed percentage
   public AnomalyDetailsResponseDTO(
           AnomlyDto anomalyDto,
           List<AnomalyTrackingDto> anomalyTrackingDto,
           long consistentCount,
           long inconsistentCount,
           double resolvedPercentage
   ) {
      this(anomalyDto, anomalyTrackingDto, consistentCount, inconsistentCount);
      this.partiallyResolvedPercentage = resolvedPercentage;
   }

   // ------- Getters & Setters -------
   public AnomlyDto getAnomalyDto() { return anomalyDto; }
   public void setAnomalyDto(AnomlyDto anomalyDto) { this.anomalyDto = anomalyDto; }

   public List<AnomalyTrackingDto> getAnomalyTrackingDto() { return anomalyTrackingDto; }
   public void setAnomalyTrackingDto(List<AnomalyTrackingDto> anomalyTrackingDto) { this.anomalyTrackingDto = anomalyTrackingDto; }

   public long getConsistentCount() { return consistentCount; }
   public void setConsistentCount(long consistentCount) { this.consistentCount = consistentCount; }

   public long getInconsistentCount() { return inconsistentCount; }
   public void setInconsistentCount(long inconsistentCount) { this.inconsistentCount = inconsistentCount;}

              // useful to return
   public long getResolvedCount() { return consistentCount; }      // alias

   public double getPartiallyResolvedPercentage() { return partiallyResolvedPercentage; }
   public void setPartiallyResolvedPercentage(double p) { this.partiallyResolvedPercentage = p; }

}
