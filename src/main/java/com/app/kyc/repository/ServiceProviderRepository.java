package com.app.kyc.repository;

import java.util.Date;
import java.util.List;
import java.util.Optional;

import com.app.kyc.model.DashboardObjectInterface;
import com.app.kyc.model.ServiceProviderStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.app.kyc.entity.ServiceProvider;

@Repository
public interface ServiceProviderRepository extends JpaRepository<ServiceProvider, Long>
{
   @Query(value = "SELECT * FROM service_providers WHERE name = ?", nativeQuery = true)
   ServiceProvider findByName(@Param("name") String name);

   List<ServiceProvider> findAllByIndustryId(Long industryId);

   @Query("select s.id as value, s.name as name from ServiceProvider s where s.industry.id = ?1")
   List<DashboardObjectInterface> findAllByIndustryIdForDashboard(Long industryId);

   @Query("select s.id from ServiceProvider s where s.industry.id = ?1")
   List<Long> findServiceProvidersIdByIndustryId(Long industryId);

   int countByIndustryIdAndCreatedOnGreaterThanAndCreatedOnLessThanEqual(Long industryId, Date start, Date end);

   @Query(value = "Select sp.id as value , sp.color as name from ServiceProvider sp where sp.id in(:serviceProviderIds)")
   List<DashboardObjectInterface> findColorsByServiceProviderIds(@Param("serviceProviderIds") List<Long> serviceProviderIds);


   @Query("select s.name from ServiceProvider s where s.id in (:ids)")
   List<String> findNamesByIds(@Param("ids") List<Long> ids);
   Optional<ServiceProvider> findByNameIgnoreCase(String name);

   Page<ServiceProvider> findAllByStatus(ServiceProviderStatus status, Pageable pageabl);  // only active ones


   boolean existsByNameIgnoreCase(String name);
}
