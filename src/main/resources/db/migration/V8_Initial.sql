CREATE TABLE `consumer_tracking` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `consumer_id` bigint DEFAULT NULL,
  `service_provider_id` bigint DEFAULT NULL,
  `consistent_on` varchar(50) DEFAULT NULL,
  `is_consistent` tinyint(1) DEFAULT '1',
  `created_on` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_consumer_tracking_consumer_id` (`consumer_id`),
  KEY `idx_consumer_tracking_service_provider_id` (`service_provider_id`),
  CONSTRAINT `fk_consumer_tracking_service_provider` FOREIGN KEY (`service_provider_id`) REFERENCES `service_providers` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci