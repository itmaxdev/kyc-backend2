package com.app.kyc.response;

import java.util.List;

import com.app.kyc.model.AnomalyTrackingDto;
import com.app.kyc.model.AnomlyDto;

public class AnomalyDetailsResponseDTO {

   private AnomlyDto anomalyDto;
   private List<AnomalyTrackingDto> anomalyTrackingDto;
   private long consistentCount;
   private long inconsistentCount;

   public AnomalyDetailsResponseDTO(AnomlyDto anomalyDto,
                                    List<AnomalyTrackingDto> anomalyTrackingDto,
                                    long consistentCount,
                                    long inconsistentCount) {
      this.anomalyDto = anomalyDto;
      this.anomalyTrackingDto = anomalyTrackingDto;
      this.consistentCount = consistentCount;
      this.inconsistentCount = inconsistentCount;
   }

   public AnomlyDto getAnomalyDto() {
      return anomalyDto;
   }

   public void setAnomalyDto(AnomlyDto anomalyDto) {
      this.anomalyDto = anomalyDto;
   }

   public List<AnomalyTrackingDto> getAnomalyTrackingDto() {
      return anomalyTrackingDto;
   }

   public void setAnomalyTrackingDto(List<AnomalyTrackingDto> anomalyTrackingDto) {
      this.anomalyTrackingDto = anomalyTrackingDto;
   }

   public long getConsistentCount() {
      return consistentCount;
   }

   public void setConsistentCount(long consistentCount) {
      this.consistentCount = consistentCount;
   }

   public long getInconsistentCount() {
      return inconsistentCount;
   }

   public void setInconsistentCount(long inconsistentCount) {
      this.inconsistentCount = inconsistentCount;
   }
}
