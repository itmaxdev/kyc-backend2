package com.app.kyc.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.app.kyc.entity.User;
import com.app.kyc.entity.UserConfig;

@Repository
public interface UserConfigRepository extends JpaRepository<UserConfig, Long> {
	 List<UserConfig> findByUser(User user);
	 Optional<UserConfig> findByUserAndSettingKey(User user, String settingKey);
}