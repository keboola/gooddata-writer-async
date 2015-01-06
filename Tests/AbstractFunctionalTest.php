<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\GoodDataWriter\Tests;


use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\Service\EventLogger;
use Keboola\GoodDataWriter\Service\Queue;
use Keboola\GoodDataWriter\Service\S3Client;
use Keboola\GoodDataWriter\Writer\JobExecutor;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Monolog\Handler\NullHandler;
use Symfony\Component\Translation\Translator;
use Syrup\ComponentBundle\Encryption\Encryptor;
use Syrup\ComponentBundle\Monolog\Uploader\SyrupS3Uploader;

class AbstractFunctionalTest extends \PHPUnit_Framework_TestCase
{

	/**
	 * @var SharedStorage
	 */
	protected $sharedStorage;
	/**
	 * @var \Monolog\Logger
	 */
	protected $logger;
	/**
	 * @var \Syrup\ComponentBundle\Filesystem\Temp
	 */
	protected $temp;
	/**
	 * @var Queue
	 */
	protected $queue;
	/**
	 * @var Translator
	 */
	protected $translator;
	/**
	 * @var S3Client
	 */
	protected $s3client;
	/**
	 * @var SyrupS3Uploader
	 */
	protected $s3uploader;

	/**
	 * @var RestApi
	 */
	protected $restApi;

	protected $scriptsPath;
	protected $userAgent;
	protected $gdConfig;

	public function setUp()
	{
		parent::setUp();

		$appName = 'gooddata-writer';
		$this->scriptsPath = __DIR__ . '/../GoodData';
		$this->userAgent = 'gooddata-writer (testing)';
		$this->gdConfig = array(
			'access_token' => GD_ACCESS_TOKEN,
			'domain' => GD_DOMAIN_NAME,
			'sso_provider' => GD_SSO_PROVIDER
		);
		$awsConfig = array(
			'access_key' => AWS_ACCESS_KEY,
			'secret_key' => AWS_SECRET_KEY,
			'region' => AWS_REGION,
			'queue_url' => AWS_QUEUE_URL
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

		$this->sharedStorage = new SharedStorage($db, $encryptor);
		$this->logger = new \Monolog\Logger($appName);
		$this->logger->pushHandler(new NullHandler());
		$this->restApi = new RestApi($appName, $this->logger);
		$this->temp = new \Syrup\ComponentBundle\Filesystem\Temp($appName);
		$this->queue = new Queue($awsConfig);
		$this->translator = new Translator('en');
		$this->s3uploader = new SyrupS3Uploader($s3Config);
		$this->s3client = new S3Client($s3Config);
	}

	public function getJob($command, $configuration, $storageApiClient)
	{
		$commandName = ucfirst($command);
		$commandClass = 'Keboola\GoodDataWriter\Job\\' . $commandName;
		if (!class_exists($commandClass)) {
			throw new \Exception(sprintf("Job '%s' does not exist", $commandName));
		}

		/**
		 * @var \Keboola\GoodDataWriter\Job\AbstractJob $command
		 */
		$command = new $commandClass($configuration, $this->gdConfig, $this->sharedStorage, $storageApiClient);
		$command->setScriptsPath($this->scriptsPath);
		$command->setEventLogger(new EventLogger($storageApiClient, $this->s3uploader));
		$command->setTranslator($this->translator);
		$command->setTemp($this->temp); //For csv handler
		$command->setLogger($this->logger); //For csv handler
		$command->setS3Client($this->s3client); //For dataset definitions and manifests
		$command->setQueue($this->queue);
		$command->setS3Uploader($this->s3uploader);

		return $command;
	}
}