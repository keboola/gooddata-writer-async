<?php
ini_set('memory_limit', '256M');

if (file_exists(__DIR__ . '/config/config.php')) {
	require_once __DIR__ . '/config/config.php';
}

function setupConst($name, $default=null)
{
	defined($name) || define($name, getenv($name)? getenv($name) : $default);
}

setupConst('STORAGE_API_URL', 'https://connection.keboola.com');
setupConst('STORAGE_API_TOKEN', 'your_token');
setupConst('DB_HOST', '127.0.0.1');
setupConst('DB_NAME', 'gooddata_writer');
setupConst('DB_USER', 'user');
setupConst('DB_PASSWORD', '');
setupConst('GD_DOMAIN_NAME', 'keboola-devel');
setupConst('GD_DOMAIN_USER', 'gooddata-devel@keboola.com');
setupConst('GD_DOMAIN_PASSWORD', '');
setupConst('ENCRYPTION_KEY', md5(uniqid()));

require_once __DIR__ . '/../vendor/autoload.php';