<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\GoodDataWriter\Tests;


use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\Service\Queue;
use Keboola\GoodDataWriter\Service\S3Client;
use Keboola\GoodDataWriter\Writer\JobExecutor;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Monolog\Handler\NullHandler;
use Symfony\Component\Translation\Translator;
use Syrup\ComponentBundle\Encryption\Encryptor;
use Syrup\ComponentBundle\Monolog\Uploader\SyrupS3Uploader;

class AbstractFunctionalTest extends \Symfony\Bundle\FrameworkBundle\Test\WebTestCase
{
	/**
	 * @var JobExecutor
	 */
	private $jobExecutor;

	public function setUp()
	{
		parent::setUp();

		$appName = 'gooddata-writer';
		$scriptsPath = 'scripts_path';
		$userAgent = '';
		$gdConfig = array(
			'access_token' => '',
			'domain' => '',
			'project_name_prefix' => '',
			'sso_provider' => '',
			'users_domain' => ''
		);
		$awsConfig = array(
			'access_key' => '',
			'secret_key' => '',
			'region' => '',
			'queue_url' => ''
		);
		$s3Config = array(
			'aws-access-key' => '',
			'aws-secret-key' => '',
			's3-upload-path' => '',
			'bitly-login' => '',
			'bitly-api-key' => ''
		);

		$encryptor = new Encryptor(md5(uniqid()));

		$db = \Doctrine\DBAL\DriverManager::getConnection(array(
			'driver' => 'pdo_mysql',
			'host' => DB_HOST,
			'dbname' => DB_NAME,
			'user' => DB_USER,
			'password' => DB_PASSWORD,
		));
		$stmt = $db->prepare(file_get_contents(__DIR__ . '/../db.sql'));
		$stmt->execute();

		$sharedStorage = new SharedStorage($db, $encryptor);
		$logger = new \Monolog\Logger($appName);
		$logger->pushHandler(new NullHandler());
		$restApi = new RestApi($appName, $logger);
		$temp = new \Syrup\ComponentBundle\Filesystem\Temp($appName);
		$queue = new Queue($awsConfig);
		$translator = new Translator('en');
		$s3uploader = new SyrupS3Uploader($s3Config);
		$s3client = new S3Client($s3Config);

		$this->jobExecutor = new JobExecutor($scriptsPath, $userAgent, $gdConfig, $sharedStorage, $restApi, $logger, $temp, $queue,
			$translator, $s3uploader, $s3client);
	}
}