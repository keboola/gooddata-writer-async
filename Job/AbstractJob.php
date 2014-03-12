<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\JobProcessException;
use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
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
	protected $preRelease;
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
	protected $logs;

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

	public function setPreRelease($preRelease)
	{
		$this->preRelease = $preRelease;
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

	protected function logEvent($event, $details, $restApiLogPath = null)
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

	protected function getWebDavUrl($bucketAttributes)
	{
		$webDavUrl = null;
		if (isset($bucketAttributes['gd']['backendUrl']) && $bucketAttributes['gd']['backendUrl'] != RestApi::DEFAULT_BACKEND_URL) {

			// Get WebDav url for non-default backend
			$backendUrl = (substr($bucketAttributes['gd']['backendUrl'], 0, 8) != 'https://'
					? 'https://' : '') . $bucketAttributes['gd']['backendUrl'];
			$this->restApi->setBaseUrl($backendUrl);
			$this->restApi->login($this->appConfiguration->gd_username, $this->appConfiguration->gd_password);
			$webDavUrl = $this->restApi->getWebDavUrl();
			if (!$webDavUrl) {
				throw new JobProcessException(sprintf("Getting of WebDav url for backend '%s' failed.", $bucketAttributes['gd']['backendUrl']));
			}
		}
		return $webDavUrl;
	}
}
