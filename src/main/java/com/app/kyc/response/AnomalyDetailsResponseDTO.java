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
   private long totalCount;            // consistent + inconsistent

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

      this.totalCount = consistentCount + inconsistentCount; // total_count

      // resolved_percentage = (resolved_count / total_count) * 100
      if (this.totalCount > 0) {
         this.partiallyResolvedPercentage = BigDecimal.valueOf(consistentCount)
                 .multiply(BigDecimal.valueOf(100))
                 .divide(BigDecimal.valueOf(this.totalCount), 2, RoundingMode.HALF_UP)
                 .doubleValue();
      } else {
         this.partiallyResolvedPercentage = 0.0;
      }
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
   public void setConsistentCount(long consistentCount) { this.consistentCount = consistentCount; recalc(); }

   public long getInconsistentCount() { return inconsistentCount; }
   public void setInconsistentCount(long inconsistentCount) { this.inconsistentCount = inconsistentCount; recalc(); }

   public long getTotalCount() { return totalCount; }              // useful to return
   public long getResolvedCount() { return consistentCount; }      // alias

   public double getPartiallyResolvedPercentage() { return partiallyResolvedPercentage; }
   public void setPartiallyResolvedPercentage(double p) { this.partiallyResolvedPercentage = p; }

   // Recalculate when counts change
   private void recalc() {
      this.totalCount = this.consistentCount + this.inconsistentCount;
      if (this.totalCount > 0) {
         this.partiallyResolvedPercentage = BigDecimal.valueOf(this.consistentCount)
                 .multiply(BigDecimal.valueOf(100))
                 .divide(BigDecimal.valueOf(this.totalCount), 2, RoundingMode.HALF_UP)
                 .doubleValue();
      } else {
         this.partiallyResolvedPercentage = 0.0;
      }
   }
}
