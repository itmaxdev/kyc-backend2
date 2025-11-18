package com.app.kyc.response;

import com.app.kyc.model.ConsumerDto;
import com.app.kyc.model.ConsumerMsidnDto;

public class ConsumersHasSubscriptionsMsisdnResponseDTO
{

   ConsumerMsidnDto consumer;
   boolean hasSubscription;

   public ConsumersHasSubscriptionsMsisdnResponseDTO(ConsumerMsidnDto consumer, boolean hasSubscription)
   {
      this.consumer = consumer;
      this.hasSubscription = hasSubscription;
   }

   public ConsumerMsidnDto getConsumer()
   {
      return consumer;
   }

   public void setConsumer(ConsumerMsidnDto consumer)
   {
      this.consumer = consumer;
   }

   public boolean isHasSubscription()
   {
      return hasSubscription;
   }

   public void setHasSubscription(boolean hasSubscription)
   {
      this.hasSubscription = hasSubscription;
   }

}
