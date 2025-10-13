package com.app.kyc.service;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.app.kyc.model.DashboardObjectInterface;
import com.app.kyc.model.ServiceProviderPutRequest;
import org.springframework.data.crossstore.ChangeSetPersister.NotFoundException;

import com.app.kyc.entity.ServiceProvider;
import com.app.kyc.response.ServiceProviderAllInfoResponseDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

public interface ServiceProviderService
{

   public ServiceProvider getServiceProviderById(Long id);

   public ServiceProviderAllInfoResponseDTO getServiceProviderAllInfoById(Long id);

   public ServiceProvider getServiceProviderByName(String name);

   public Map<String, Object> getAllServiceProviders(String params) throws JsonMappingException, JsonProcessingException;

   public List<ServiceProvider> getAllServiceProviders();

   public void addServiceProvider(ServiceProvider serviceProvider);

   public ServiceProvider updateServiceProvider(ServiceProvider serviceProvider) throws NotFoundException;

   public void deleteServiceProvider(Long id);

   public void deleteData(Long serviceProvider);

   List<DashboardObjectInterface> getAllServiceProvidersByIndustryIdForDashboard(Long industryId);

   int countServiceProvidersByIndustryId(Long industryId, Date start, Date end);

   public void activateService(Long serviceProviderId, Long userId);

   public void deactivateService(Long serviceProviderId);

   public Map<String, Object> getAllServiceProvidersByIndustry(Long userId);

   List<Long> getAllServiceProvidersIdByIndustryId(Long industryId);

   List<DashboardObjectInterface> findColorsByServiceProviderIds(List<Long> serviceProviderIds);

   ServiceProvider createServiceProvider(ServiceProvider sp);

    Object update(Long id, ServiceProviderPutRequest req);
}
