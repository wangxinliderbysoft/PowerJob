ALTER TABLE `instance_info` ADD COLUMN `extend_value` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL AFTER `expected_trigger_time`;

ALTER TABLE `instance_info` ADD COLUMN `meta` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL AFTER `last_report_time`;

ALTER TABLE `instance_info` ADD COLUMN `outer_key` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL AFTER `meta`;

ALTER TABLE `instance_info` ADD INDEX `idx04_instance_info_outer_key`(`outer_key` ASC) USING BTREE;