<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\Service\EventLogger;
use Keboola\GoodDataWriter\Service\Queue;
use Keboola\GoodDataWriter\Writer\AppConfiguration;
use Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\GoodDataWriter\Writer\SharedConfig,
	Keboola\GoodDataWriter\GoodData\RestApi,
	Keboola\GoodDataWriter\Service\S3Client,
	Keboola\StorageApi\Client as StorageApiClient;
use Monolog\Logger;
use Symfony\Component\Translation\TranslatorInterface;
use Syrup\ComponentBundle\Filesystem\TempService;

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
	 * @var S3Client
	 */
	protected $s3Client;
	/**
	 * For CsvHandler
	 * @var TempService
	 */
	protected $tempService;
	/**
	 * @var TranslatorInterface
	 */
	protected $translator;
	/**
	 * For CsvHandler
	 * @var Logger
	 */
	protected $logger;
	/**
	 * @var StorageApiClient $storageApiClient
	 */
	protected $storageApiClient;
	/**
	 * @var Queue
	 */
	protected $queue;
	/**
	 * @var \Keboola\GoodDataWriter\GoodData\User
	 */
	private $domainUser;
	/**
	 * @var \Keboola\GoodDataWriter\Service\EventLogger
	 */
	protected $eventLogger;

	protected $logs;

	private $tmpDir;
	protected $scriptsPath;


	public function __construct(Configuration $configuration, AppConfiguration $appConfiguration, SharedConfig $sharedConfig,
	                            S3Client $s3Client, TranslatorInterface $translator, StorageApiClient $storageApiClient,
								EventLogger $eventLogger)
	{
		$this->configuration = $configuration;
		$this->appConfiguration = $appConfiguration;
		$this->sharedConfig = $sharedConfig;
		$this->s3Client = $s3Client;
		$this->translator = $translator;
		$this->storageApiClient = $storageApiClient;
		$this->eventLogger = $eventLogger;

		$this->logs = array();
	}


	abstract function prepare($params);
	abstract function run($job, $params, RestApi $restApi);


	protected function getTmpDir($jobId)
	{
		if (!$this->tmpDir) {
			$this->tmpDir = sprintf('%s/%s', $this->appConfiguration->tmpPath, $jobId);
			if (!file_exists($this->appConfiguration->tmpPath)) mkdir($this->appConfiguration->tmpPath);
			if (!file_exists($this->tmpDir)) mkdir($this->tmpDir);
		}
		return $this->tmpDir;
	}

	protected function getDomainUser()
	{
		if (!$this->domainUser) {
			$this->domainUser = $this->sharedConfig->getDomainUser($this->configuration->gdDomain?
				$this->configuration->gdDomain : $this->appConfiguration->gd_domain);
		}
		return $this->domainUser;
	}


	public function setTempService($tempService)
	{
		$this->tempService = $tempService;
	}

	public function setEventLogger($logger)
	{
		$this->eventLogger = $logger;
	}

	public function setLogger($logger)
	{
		$this->logger = $logger;
	}

	public function setQueue(Queue $queue)
	{
		$this->queue = $queue;
	}

	public function getLogs()
	{
		return $this->logs;
	}

	public function logEvent($message, $jobId, $runId, $params=array(), $duration=null)
	{
		$this->eventLogger->log($jobId, $runId, $message, $params, $duration);
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
				throw new WrongConfigurationException($this->translator->trans('parameters.required %1', array('%1' => $k)));
			}
		}
	}

	protected function checkWriterExistence($writerId)
	{
		$tokenInfo = $this->storageApiClient->getLogData();
		$projectId = $tokenInfo['owner']['id'];

		if (!$this->sharedConfig->writerExists($projectId, $writerId)) {
			throw new WrongConfigurationException($this->translator->trans('parameters.writerId.not_found'));
		}
	}

}
