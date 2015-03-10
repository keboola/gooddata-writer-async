<?php
use Doctrine\Common\Annotations\AnnotationRegistry;
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

defined('GW_APP_NAME') || define('GW_APP_NAME', getenv('GW_APP_NAME')? getenv('GW_APP_NAME') : 'gooddata-writer');
defined('GW_STORAGE_API_TOKEN') || define('GW_STORAGE_API_TOKEN', getenv('GW_STORAGE_API_TOKEN')? getenv('GW_STORAGE_API_TOKEN') : 'token');
defined('GW_AWS_ACCESS_KEY') || define('GW_AWS_ACCESS_KEY', getenv('GW_AWS_ACCESS_KEY')? getenv('GW_AWS_ACCESS_KEY') : '');
defined('GW_AWS_SECRET_KEY') || define('GW_AWS_SECRET_KEY', getenv('GW_AWS_SECRET_KEY')? getenv('GW_AWS_SECRET_KEY') : '');
defined('GW_AWS_S3_BUCKET') || define('GW_AWS_S3_BUCKET', getenv('GW_AWS_S3_BUCKET')? getenv('GW_AWS_S3_BUCKET') : 'keboola-logs/debug-files');
defined('GW_AWS_SQS_URL') || define('GW_AWS_SQS_URL', getenv('GW_AWS_SQS_URL')? getenv('GW_AWS_SQS_URL') : 'http://127.0.0.1:9200');
defined('GW_DB_HOST') || define('GW_DB_HOST', getenv('GW_DB_HOST')? getenv('GW_DB_HOST') : '127.0.0.1');
defined('GW_DB_NAME') || define('GW_DB_NAME', getenv('GW_DB_NAME')? getenv('GW_DB_NAME') : 'gooddata_writer');
defined('GW_DB_PASSWORD') || define('GW_DB_PASSWORD', getenv('GW_DB_PASSWORD')? getenv('GW_DB_PASSWORD') : '');
defined('GW_DB_PORT') || define('GW_DB_PORT', getenv('GW_DB_PORT')? getenv('GW_DB_PORT') : 3306);
defined('GW_DB_USER') || define('GW_DB_USER', getenv('GW_DB_USER')? getenv('GW_DB_USER') : 'user');
defined('GW_ENCRYPTION_KEY') || define('GW_ENCRYPTION_KEY', getenv('GW_ENCRYPTION_KEY')? getenv('GW_ENCRYPTION_KEY') : md5(uniqid()));
defined('GW_ELASTICSEARCH_HOST') || define('GW_ELASTICSEARCH_HOST', getenv('GW_ELASTICSEARCH_HOST')? getenv('GW_ELASTICSEARCH_HOST') : 'http://127.0.0.1:9200');
defined('GW_GD_ACCESS_TOKEN') || define('GW_GD_ACCESS_TOKEN', getenv('GW_GD_ACCESS_TOKEN')? getenv('GW_GD_ACCESS_TOKEN') : '');
defined('GW_GD_DOMAIN_UID') || define('GW_GD_DOMAIN_UID', getenv('GW_GD_DOMAIN_UID')? getenv('GW_GD_DOMAIN_UID') : 'f6bd467fe6be86df131d3b285a35c805');
defined('GW_GD_DOMAIN_NAME') || define('GW_GD_DOMAIN_NAME', getenv('GW_GD_DOMAIN_NAME')? getenv('GW_GD_DOMAIN_NAME') : 'keboola-devel');
defined('GW_GD_DOMAIN_USER') || define('GW_GD_DOMAIN_USER', getenv('GW_GD_DOMAIN_USER')? getenv('GW_GD_DOMAIN_USER') : 'gooddata-devel@keboola.com');
defined('GW_GD_DOMAIN_PASSWORD') || define('GW_GD_DOMAIN_PASSWORD', getenv('GW_GD_DOMAIN_PASSWORD')? getenv('GW_GD_DOMAIN_PASSWORD') : '');
defined('GW_GD_SSO_PROVIDER') || define('GW_GD_SSO_PROVIDER', getenv('GW_GD_SSO_PROVIDER')? getenv('GW_GD_SSO_PROVIDER') : 'dev.keboola.com');
defined('GW_OTHER_DOMAIN_USER') || define('GW_OTHER_DOMAIN_USER', getenv('GW_OTHER_DOMAIN_USER')? getenv('GW_OTHER_DOMAIN_USER') : 'user@email.com');
define('GW_SCRIPTS_PATH', realpath(__DIR__ . '/../GoodData'));

$loader = require_once __DIR__ . '/../vendor/autoload.php';

$paramsYaml = \Symfony\Component\Yaml\Yaml::dump([
    'framework' => [
        'translator' => [ 'fallback' => "en" ]
    ],
    'parameters' => [
        'app_name' => GW_APP_NAME,
        'secret' => md5(uniqid()),
        'encryption_key' => GW_ENCRYPTION_KEY,
        'database_driver' => 'pdo_mysql',
        'database_port' => GW_DB_PORT,
        'database_host' => GW_DB_HOST,
        'database_user' => GW_DB_USER,
        'database_password' => GW_DB_PASSWORD,
        'database_name' => GW_DB_NAME,
        'syrup.driver' => 'pdo_mysql',
        'syrup.port' => GW_DB_PORT,
        'syrup.host' => GW_DB_HOST,
        'syrup.user' => GW_DB_USER,
        'syrup.password' => GW_DB_PASSWORD,
        'syrup.name' => GW_DB_NAME,
        'locks_db.driver' => 'pdo_mysql',
        'locks_db.port' => GW_DB_PORT,
        'locks_db.host' => GW_DB_HOST,
        'locks_db.user' => GW_DB_USER,
        'locks_db.password' => GW_DB_PASSWORD,
        'locks_db.name' => GW_DB_NAME,
        'uploader' => [
            'aws-access-key' => GW_AWS_ACCESS_KEY,
            'aws-secret-key' => GW_AWS_SECRET_KEY,
            's3-upload-path' => GW_AWS_S3_BUCKET
        ],
        'storage_api.url' => 'https://connection.keboola.com/',
        'storage_api.test.url' => 'https://connection.keboola.com/',
        'storage_api.test.token' => GW_STORAGE_API_TOKEN,
        'elasticsearch' => [
            'hosts' => [GW_ELASTICSEARCH_HOST]
        ],
        'elasticsearch.index_prefix' => 'devel',
        'queue' => [
            'url' => null,
            'db_table' => 'queues'
        ],
        'gdwr_gd' => [
            'access_token' => GW_GD_ACCESS_TOKEN,
            'domain' =>  GW_GD_DOMAIN_NAME,
            'project_name_prefix' => 'DEV',
            'sso_provider' => GW_GD_SSO_PROVIDER,
            'users_domain' => 'clients.dev.keboola.com'
        ],
        'gdwr_invitations' => [
            'domain' => GW_GD_DOMAIN_NAME,
            'email' =>'gooddata-robot@keboola.com',
            'password' => ''
        ],
        'gdwr_key_passphrase' => '',
        'gdwr_scripts_path' => GW_SCRIPTS_PATH,
        'components' => [
            'gooddata-writer' => [
                'bundle' => 'Keboola\GoodDataWriter\KeboolaGoodDataWriter'
            ]
        ]
    ]
]);
file_put_contents(__DIR__ . '/../vendor/keboola/syrup/app/config/parameters.yml', $paramsYaml);
touch(__DIR__ . '/../vendor/keboola/syrup/app/config/parameters_shared.yml');

$db = \Doctrine\DBAL\DriverManager::getConnection([
    'driver' => 'pdo_mysql',
    'host' => GW_DB_HOST,
    'dbname' => GW_DB_NAME,
    'user' => GW_DB_USER,
    'password' => GW_DB_PASSWORD,
    'port' => GW_DB_PORT
]);

$stmt = $db->prepare(file_get_contents(__DIR__ . '/../db.sql'));
$stmt->execute();
$stmt->closeCursor();

$stmt = $db->prepare(file_get_contents(__DIR__ . '/../vendor/keboola/syrup/tests/db.sql'));
$stmt->execute();
$stmt->closeCursor();

$encryptor = new Encryptor(GW_ENCRYPTION_KEY);
$db->insert('domains', [
    'name' => GW_GD_DOMAIN_NAME,
    'username' => GW_GD_DOMAIN_USER,
    'password' => $encryptor->encrypt(GW_GD_DOMAIN_PASSWORD),
    'uid' => GW_GD_DOMAIN_UID
]);
$db->insert('queues', [
    'id' => 'default',
    'access_key' => GW_AWS_ACCESS_KEY,
    'secret_key' => GW_AWS_SECRET_KEY,
    'region' => 'us-east-1',
    'url' => GW_AWS_SQS_URL
]);
$db->close();

/** To make annotations work here */
AnnotationRegistry::registerAutoloadNamespaces([
    'Sensio\\Bundle\\FrameworkExtraBundle' => 'vendor/sensio/framework-extra-bundle/'
]);

passthru('php vendor/sensio/distribution-bundle/Sensio/Bundle/DistributionBundle/Resources/bin/build_bootstrap.php '
    . 'vendor/keboola/syrup/app vendor/keboola/syrup/app');
passthru('php vendor/keboola/syrup/app/console cache:clear --env=test');
passthru('php vendor/keboola/syrup/app/console syrup:create-index -d  --env=test');
