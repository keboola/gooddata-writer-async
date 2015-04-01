<?php
use Keboola\Syrup\Encryption\Encryptor;

ini_set('memory_limit', '256M');

set_error_handler('exceptions_error_handler');
function exceptions_error_handler($severity, $message, $filename, $lineno)
{
    if (error_reporting() == 0) {
        return;
    }
    if (error_reporting() & $severity) {
        throw new ErrorException($message, 0, $severity, $filename, $lineno);
    }
}

if (file_exists(__DIR__ . '/config/config.php')) {
    require_once __DIR__ . '/config/config.php';
}

function setupConst($name, $default = null)
{
    defined($name) || define($name, getenv($name)? getenv($name) : $default);
}

setupConst('STORAGE_API_URL', 'https://connection.keboola.com');
setupConst('STORAGE_API_TOKEN', 'your_token');
setupConst('AWS_ACCESS_KEY', '');
setupConst('AWS_SECRET_KEY', '');
setupConst('AWS_REGION', 'us-east-1');
setupConst('AWS_QUEUE_URL', '');
setupConst('DB_HOST', '127.0.0.1');
setupConst('DB_NAME', 'gooddata_writer');
setupConst('DB_PASSWORD', '');
setupConst('DB_PORT', 3306);
setupConst('DB_USER', 'user');
setupConst('ENCRYPTION_KEY', md5(uniqid()));
setupConst('GD_ACCESS_TOKEN', '');
setupConst('GD_DOMAIN_UID', 'f6bd467fe6be86df131d3b285a35c805');
setupConst('GD_DOMAIN_NAME', 'keboola-devel');
setupConst('GD_DOMAIN_USER', 'gooddata-devel@keboola.com');
setupConst('GD_DOMAIN_PASSWORD', '');
setupConst('GD_SSO_PROVIDER', 'dev.keboola.com');

require_once __DIR__ . '/../vendor/autoload.php';

$db = \Doctrine\DBAL\DriverManager::getConnection(array(
    'driver' => 'pdo_mysql',
    'host' => DB_HOST,
    'dbname' => DB_NAME,
    'user' => DB_USER,
    'password' => DB_PASSWORD,
    'port' => DB_PORT
));

$stmt = $db->prepare(file_get_contents(__DIR__ . '/../db.sql'));
$stmt->execute();
$stmt->closeCursor();

$encryptor = new Encryptor(ENCRYPTION_KEY);
$db->insert('domains', array('name' => GD_DOMAIN_NAME, 'username' => GD_DOMAIN_USER, 'password' => $encryptor->encrypt(GD_DOMAIN_PASSWORD), 'uid' => GD_DOMAIN_UID));

