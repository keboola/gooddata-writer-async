<?php
ini_set('memory_limit', '256M');

if (file_exists(__DIR__ . '/config/config.php')) {
	require_once __DIR__ . '/config/config.php';
}

require_once __DIR__ . '/../vendor/autoload.php';