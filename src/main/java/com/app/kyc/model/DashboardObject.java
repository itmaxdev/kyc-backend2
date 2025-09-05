package com.app.kyc.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Objects;

public class DashboardObject implements DashboardObjectInterface
{

   @JsonInclude(JsonInclude.Include.NON_NULL)
   String name;

   @JsonInclude(JsonInclude.Include.NON_NULL)
   Integer value;

   @JsonInclude(JsonInclude.Include.NON_NULL)
   List<DashboardObject> values;

   @JsonInclude(JsonInclude.Include.NON_NULL)
   Long serviceproviderid;
   
   public DashboardObject() {}

   public DashboardObject(String name, Integer value) {
      this.name = name;
      this.value = value;
   }

   public DashboardObject(String name, Integer value, Long serviceProviderId) {
      this.name = name;
      this.value = value;
      this.serviceproviderid = serviceProviderId;
   }

   public DashboardObject(String name, List<DashboardObject> values) {
      this.name = name;
      this.values = values;
   }
   
   @Override
   public String getName() {
      return this.name;
   }

   @Override
   public Integer getValue() {
      return this.value;
   }

   @Override
   public Long getServiceproviderid() {
      return this.serviceproviderid;
   }

   @Override
   public List<DashboardObject> getValues() {
      return this.values;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DashboardObject that = (DashboardObject) o;
      return Objects.equals(name, that.name);
   }

   @Override
   public int hashCode() {
      return Objects.hash(name);
   }
}