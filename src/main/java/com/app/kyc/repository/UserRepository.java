package com.app.kyc.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.app.kyc.entity.User;

@Repository
public interface UserRepository extends JpaRepository<User, Long>
{
   @Query(value = "SELECT * FROM users WHERE email = ?1", nativeQuery = true)
   User findByEmail(@Param("email") String email);

   Page<User> findAllByCreatedBy(Pageable pageable, Long createdById);

   List<User> findAllByServiceProviderId(Long serviceProviderId);
}
