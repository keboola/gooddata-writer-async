<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
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
	 * @var TempService
	 */
	protected $tempService;
	/**
	 * @var TranslatorInterface
	 */
	protected $translator;
	/**
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

	/**
	 * @var \SplFileObject
	 */
	private $logFile;
	protected $logs;

	private $tmpDir;
	protected $scriptsPath;


	public function __construct(Configuration $configuration, AppConfiguration $appConfiguration, SharedConfig $sharedConfig,
	                            S3Client $s3Client, TranslatorInterface $translator, StorageApiClient $storageApiClient)
	{
		$this->configuration = $configuration;
		$this->appConfiguration = $appConfiguration;
		$this->sharedConfig = $sharedConfig;
		$this->s3Client = $s3Client;
		$this->translator = $translator;
		$this->storageApiClient = $storageApiClient;

		$this->initLog();
		$this->logs = array();
	}


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

	public function logEvent($event, $details, $restApiLogPath=null, $message=null, $jobId=null, $runId=null)
	{
		if ($message && $jobId && $runId) {
			$this->eventLogger->log($jobId, $runId, $message);
		}
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
				throw new WrongConfigurationException($this->translator->trans('parameters.required %1', array('%1' => $k)));
			}
		}
	}

}
