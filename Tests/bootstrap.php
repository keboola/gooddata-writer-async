<?php
ini_set('memory_limit', '256M');

if (file_exists(__DIR__ . '/config/config.php')) {
	require_once __DIR__ . '/config/config.php';
}


defined('STORAGE_API_URL')
|| define('STORAGE_API_URL', getenv('STORAGE_API_URL') ? getenv('STORAGE_API_URL') : 'https://connection.keboola.com');

defined('STORAGE_API_TOKEN')
|| define('STORAGE_API_TOKEN', getenv('STORAGE_API_TOKEN') ? getenv('STORAGE_API_TOKEN') : 'your_token');


defined('DB_HOST')
|| define('DB_HOST', getenv('DB_HOST') ? getenv('DB_HOST') : '127.0.0.1');

defined('DB_NAME')
|| define('DB_NAME', getenv('DB_NAME') ? getenv('DB_NAME') : 'gooddata_writer');

defined('DB_USER')
|| define('DB_USER', getenv('DB_USER') ? getenv('DB_USER') : 'user');

defined('DB_PASSWORD')
|| define('DB_PASSWORD', getenv('DB_PASSWORD') ? getenv('DB_PASSWORD') : '');

require_once __DIR__ . '/../vendor/autoload.php';