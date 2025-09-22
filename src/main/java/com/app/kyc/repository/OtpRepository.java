package com.app.kyc.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.app.kyc.entity.Otp;
import com.app.kyc.enums.Channel;
import com.app.kyc.enums.OtpPurpose;

@Repository
public interface OtpRepository extends JpaRepository<Otp, Long>
{
	Optional<Otp> findByUserIdAndChannelAndPurpose(Long userId, Channel channel, OtpPurpose purpose);
}
