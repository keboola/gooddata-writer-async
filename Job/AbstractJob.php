<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\JobProcessException;
use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\Service\Queue;
use Keboola\GoodDataWriter\Writer\AppConfiguration;
use Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\GoodDataWriter\Writer\SharedConfig,
	Keboola\GoodDataWriter\GoodData\RestApi,
	Keboola\GoodDataWriter\Service\S3Client,
	Keboola\StorageApi\Client as StorageApiClient;
use Syrup\ComponentBundle\Filesystem\TempServiceFactory;

abstract class AbstractJob
{
	/**
	 * @var Configuration
	 */
	protected $configuration;
	/**
	 * @var AppConfiguration
	 */
	protected $appConfiguration;
	/**
	 * @var SharedConfig
	 */
	protected $sharedConfig;
	/**
	 * @var RestApi
	 */
	protected $restApi;
	/**
	 * @var S3Client
	 */
	protected $s3Client;
	/**
	 * @var \Monolog\Logger
	 */
	protected $logger;
	/**
	 * @var TempServiceFactory
	 */
	protected $tempServiceFactory;

	/**
	 * @var StorageApiClient $storageApiClient
	 */
	protected $storageApiClient;

	protected $tmpDir;
	protected $rootPath;
	protected $scriptsPath;
	/**
	 * @var \Keboola\GoodDataWriter\GoodData\User
	 */
	protected $domainUser;

	/**
	 * @var \Syrup\ComponentBundle\Filesystem\TempService
	 */
	private $tempService;
	/**
	 * @var \SplFileObject
	 */
	private $logFile;
	/**
	 * @var Queue
	 */
	protected $queue;

	protected $logs;

	protected $preRelease;
	protected $isTesting;

	public function __construct(Configuration $configuration, AppConfiguration $appConfiguration, SharedConfig $sharedConfig,
								RestApi $restApi, S3Client $s3Client, TempServiceFactory $tempServiceFactory)
	{
		$this->configuration = $configuration;
		$this->appConfiguration = $appConfiguration;
		$this->sharedConfig = $sharedConfig;
		$this->restApi = $restApi;
		$this->s3Client = $s3Client;
		$this->tempServiceFactory = $tempServiceFactory;
		$this->tempService = $tempServiceFactory->get('gooddata_writer');

		$this->domainUser = $this->sharedConfig->getDomainUser($appConfiguration->gd_domain);

		$this->initLog();
		$this->logs = array();
	}


	abstract function run($job, $params);



	public function setTmpDir($tmpDir)
	{
		$this->tmpDir = $tmpDir;
	}

	public function setScriptsPath($scriptsPath)
	{
		$this->scriptsPath = $scriptsPath;
	}

	public function setLogger($logger)
	{
		$this->logger = $logger;
	}

	public function setStorageApiClient($storageApiClient)
	{
		$this->storageApiClient = $storageApiClient;
	}

	public function setQueue(Queue $queue)
	{
		$this->queue = $queue;
	}

	public function setPreRelease($preRelease)
	{
		$this->preRelease = $preRelease;
	}

	public function setIsTesting($isTesting)
	{
		$this->isTesting = $isTesting;
	}


	public function initLog()
	{
		$this->logFile = $this->tempService->createTmpFile('.json')->openFile('a');
		$this->logFile->fwrite('[');
	}

	public function getLogPath()
	{
		$this->logFile->fwrite('null]');
		return $this->logFile->getRealPath();
	}

	public function getLogs()
	{
		return $this->logs;
	}

	public function logEvent($event, $details, $restApiLogPath = null)
	{
		$this->logFile->fwrite('{"' . $event . '": ');
		$details = json_encode(array_merge(array('time' => date('c')), $details), JSON_PRETTY_PRINT);
		if ($restApiLogPath && file_exists($restApiLogPath)) {
			$this->logFile->fwrite(rtrim($details, '}') . ', "restApi": ');
			shell_exec(sprintf('cat %s >> %s', escapeshellarg($restApiLogPath), escapeshellarg($this->logFile->getRealPath())));
			$this->logFile->fwrite('}');
		} else {
			$this->logFile->fwrite($details);
		}
		$this->logFile->fwrite('},');
	}

	/**
	 * @param array $params
	 * @param array $required
	 * @throws WrongConfigurationException
	 */
	protected function checkParams($params, $required)
	{
		foreach($required as $k) {
			if (empty($params[$k])) {
				throw new WrongConfigurationException("Parameter '" . $k . "' is missing");
			}
		}
	}

}
