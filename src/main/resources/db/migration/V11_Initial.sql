CREATE TABLE `users_unmask_session` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `active` bit(1) NOT NULL,
  `unmask_expiry` datetime(6) DEFAULT NULL,
  `unmask_start` datetime(6) DEFAULT NULL,
  `user_id` bigint DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `FKmxlc4b0dxlobuc093wit0ex3w` (`user_id`),
  CONSTRAINT `FKmxlc4b0dxlobuc093wit0ex3w` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci