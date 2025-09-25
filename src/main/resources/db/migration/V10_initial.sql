CREATE TABLE `user_config_settings` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `user_id` bigint NOT NULL,
  `setting_key` varchar(100) NOT NULL,
  `setting_value` bigint NOT NULL,
  `created_on` datetime DEFAULT CURRENT_TIMESTAMP,
  `created_by` bigint DEFAULT NULL,
  `updated_on` datetime DEFAULT NULL,
  `updated_by` bigint DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_user_setting` (`user_id`,`setting_key`),
  CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci


INSERT INTO `user_config_settings` VALUES (1, 'LOGIN_OTP_MINUTE', 10, '2025-09-24 12:18:08', 1, '2025-09-24 12:55:08', 1);
INSERT INTO `user_config_settings` VALUES (1, 'UNMASK_OTP_MINUTE', 5, '2025-09-24 12:18:08', 1, '2025-09-24 12:55:08', 1);
INSERT INTO `user_config_settings` VALUES (1, 'UNMASK_MINUTE', 2, '2025-09-24 12:18:08', 1, '2025-09-24 12:55:08', 1);