-- Insert role
INSERT INTO `roles` (`name`, `created_on`) 
VALUES ('System', '2025-09-10 19:20:30');

-- Insert system user using the role_id of 'System'
INSERT INTO `users` (
    `email`, `password`, `first_name`, `last_name`, `phone`, `code`, 
    `code_expiry`, `status`, `government_id`, `last_login`, `deleted`, 
    `created_on`, `department`, `industry_id`, `role_id`, `created_by`, `service_provider_id`
)
VALUES (
    'system@test.com',
    '$2a$10$QymjTcaGlJIOhVogNKBM2.Q7YnzYqebdG2pY3qT8vjbYx.sW3piKW',
    'System',
    '',
    '111',
    NULL,
    NULL,
    1,
    'e',
    NULL,
    0,
    '2025-09-10 20:00:00',
    '',
    19,
    (SELECT id FROM roles WHERE name = 'System' LIMIT 1),
    1,
    28
);

