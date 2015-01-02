<?php
use Syrup\ComponentBundle\Encryption\Encryptor;

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

$db = \Doctrine\DBAL\DriverManager::getConnection(array(
	'driver' => 'pdo_mysql',
	'host' => DB_HOST,
	'dbname' => DB_NAME,
	'user' => DB_USER,
	'password' => DB_PASSWORD,
));

$stmt = $db->prepare(file_get_contents(__DIR__ . '/../db.sql'));
$stmt->execute();
$stmt->closeCursor();

$encryptor = new Encryptor(ENCRYPTION_KEY);
$db->insert('domains', array('name' => GD_DOMAIN_NAME, 'username' => GD_DOMAIN_USER, 'password' => $encryptor->encrypt(GD_DOMAIN_PASSWORD)));