CREATE TABLE `users_otp` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `channel` varchar(255) DEFAULT NULL,
  `expiry_date` datetime(6) DEFAULT NULL,
  `otp_code` varchar(255) DEFAULT NULL,
  `purpose` varchar(255) DEFAULT NULL,
  `user_id` bigint DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `FKsefoouvge5c0mfvrtykb79tgk` (`user_id`),
  CONSTRAINT `FKsefoouvge5c0mfvrtykb79tgk` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci