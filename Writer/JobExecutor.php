<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-02
 */

namespace Keboola\GoodDataWriter\Writer;

use Keboola\GoodDataWriter\Exception\JobProcessException,
	Keboola\GoodDataWriter\Exception\ClientException,
	Keboola\GoodDataWriter\GoodData\CLToolApiErrorException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\StorageApi\ClientException as StorageApiClientException;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\Service\S3Client;
use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Event as StorageApiEvent,
	Keboola\StorageApi\Exception as StorageApiException;
use Keboola\GoodDataWriter\Service\Lock;
use Monolog\Logger;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Syrup\ComponentBundle\Filesystem\TempServiceFactory;


class QueueUnavailableException extends \Exception
{

}


class JobExecutor
{
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
	 * @var Logger
	 */
	protected $logger;
	/**
	 * @var \Syrup\ComponentBundle\Filesystem\TempServiceFactory
	 */
	protected $tempServiceFactory;

	/**
	 * @var StorageApiClient
	 */
	protected $storageApiClient;
	/**
	 * @var StorageApiEvent
	 */
	protected $storageApiEvent;


	/**
	 * @param SharedConfig $sharedConfig
	 * @param ContainerInterface $container
	 */
	public function __construct(AppConfiguration $appConfiguration, SharedConfig $sharedConfig, RestApi $restApi, Logger $logger, TempServiceFactory $tempServiceFactory)
	{
		if (!defined('JSON_PRETTY_PRINT')) {
			// fallback for PHP <= 5.3
			define('JSON_PRETTY_PRINT', 0);
		}

		$this->appConfiguration = $appConfiguration;
		$this->sharedConfig = $sharedConfig;
		$this->restApi = $restApi;
		$this->logger = $logger;
		$this->tempServiceFactory = $tempServiceFactory;

		$this->storageApiEvent = new StorageApiEvent();
	}

	public function runBatch($batchId, $force = false)
	{
		$jobs = $this->sharedConfig->fetchBatch($batchId);
		if (!count($jobs)) {
			throw new JobProcessException("Batch {$batchId} not found");
		}
		$batch = $this->sharedConfig->batchToApiResponse($batchId);

		// Batch already executed?
		if (!$force && $batch['status'] != SharedConfig::JOB_STATUS_WAITING) {
			return;
		}

		$lock = new Lock(new \PDO(sprintf('mysql:host=%s;dbname=%s', $this->appConfiguration->db_host, $this->appConfiguration->db_name),
			$this->appConfiguration->db_user, $this->appConfiguration->db_password), $batch['queueId']);

		if (!$lock->lock()) {
			throw new QueueUnavailableException("Batch {$batchId} cannot be executed, another job already in progress in the same queue.");
		}

		foreach ($jobs as $job) {
			$this->runJob($job['id']);
		}

		$lock->unlock();
	}

	/**
	 * Job execution
	 * Performs execution of job tasks and logging
	 * @param $jobId
	 * @param bool $force
	 * @throws \Keboola\GoodDataWriter\Exception\JobProcessException
	 */
	public function runJob($jobId, $force = false)
	{
		$job = $this->sharedConfig->fetchJob($jobId);
		if (!$job) {
			throw new JobProcessException("Job $jobId not found");
		}

		// Job already executed?
		if (!$force && $job['status'] != SharedConfig::JOB_STATUS_WAITING) {
			return;
		}

		$startTime = time();

		$jobData = array('result' => array());
		$logParams = $job['parameters'];
		try {
			$this->storageApiClient = new StorageApiClient(
				$job['token'],
				$this->appConfiguration->storageApiUrl,
				$this->appConfiguration->userAgent
			);
			$this->storageApiClient->setRunId($jobId);

			try {
				if ($job['parameters']) {
					$parameters = json_decode($job['parameters'], true);
					if ($parameters === false) {
						throw new JobProcessException("Parameters decoding failed");
					}
				} else {
					$parameters = array();
				}

				$tmpDir = sprintf('%s/%s', $this->appConfiguration->tmpPath, $job['id']);
				if (!file_exists($this->appConfiguration->tmpPath)) mkdir($this->appConfiguration->tmpPath);
				if (!file_exists($tmpDir)) mkdir($tmpDir);

				$configuration = new Configuration($this->storageApiClient, $job['writerId'], $this->appConfiguration->scriptsPath);
				$bucketAttributes = $configuration->bucketAttributes();
				if (!empty($bucketAttributes['maintenance'])) {
					throw new QueueUnavailableException('Writer is undergoing maintenance');
				}

				$this->sharedConfig->saveJob($jobId, array(
					'status' => 'processing',
					'startTime' => date('c', $startTime),
					'endTime' => null
				));
				$logParams = $parameters;
				$this->logEvent('start', $job, array('command' => $job['command'], 'params' => $logParams));

				$commandName = ucfirst($job['command']);
				$commandClass = 'Keboola\GoodDataWriter\Job\\' . $commandName;
				if (!class_exists($commandClass)) {
					throw new JobProcessException(sprintf('Command %s does not exist', $commandName));
				}

				$s3Client = new S3Client(
					$this->appConfiguration,
					$job['projectId'] . '.' . $job['writerId']
				);

				if (isset($bucketAttributes['gd']['backendUrl'])) {
					$this->restApi->setBaseUrl($bucketAttributes['gd']['backendUrl']);
				}
				$this->restApi->setJobId($job['id']);
				$this->restApi->initLog();
				$token = $this->storageApiClient->getLogData();

				/**
				 * @var \Keboola\GoodDataWriter\Job\AbstractJob $command
				 */
				$command = new $commandClass($configuration, $this->appConfiguration, $this->sharedConfig, $this->restApi, $s3Client, $this->tempServiceFactory);
				$command->setTmpDir($tmpDir);
				$command->setScriptsPath($this->appConfiguration->scriptsPath);
				$command->setLogger($this->logger);
				$command->setStorageApiClient($this->storageApiClient);
				$command->setPreRelease(!empty($token['owner']['features']) && in_array('rc-writer', $token['owner']['features']));

				$error = null;

				try {
					$jobData['result'] = $command->run($job, $parameters);
				} catch (\Exception $e) {
					$data = array();

					if ($e instanceof RestApiException) {
						$jobData['result']['error'] = 'Rest API Error. ' . $e->getMessage();
						$data['details'] = $e->getDetails();
					} elseif ($e instanceof CLToolApiErrorException) {
						$jobData['result']['error'] = 'CL Tool Error. ' . $e->getMessage();
						$data['details'] = $e->getData();
					} elseif ($e instanceof StorageApiClientException) {
						$jobData['result']['error'] = 'Storage API Error. ' . $e->getMessage();
					} elseif ($e instanceof ClientException) {
						$jobData['result']['error'] = $e->getMessage();
						$data['details'] = $e->getData();
					} else {
						throw $e;
					}

					$data['message'] = $e->getMessage();
					$data['trace'] = $e->getTrace();
					$jobData['result']['details'] = $s3Client->uploadString($job['id'] . '/debug-data.json', json_encode($data, JSON_PRETTY_PRINT));
				}

				$jobData['log'] = $s3Client->uploadFile($command->getLogPath(), 'text/plain', $job['id'] . '/log.json');

			} catch (\Exception $e) {
				if ($e instanceof QueueUnavailableException) {
					throw $e;
				} else {
					$this->logger->alert('Job execution error', array(
						'jobId' => $job,
						'exception' => $e,
						'runId' => $this->storageApiClient->getRunId()
					));
					$jobData['result']['error'] = 'Application error';
				}
			}

		} catch(StorageApiException $e) {
			$jobData['result']['error'] = "Storage API error: " . $e->getMessage();
		}

		$jobData['status'] = empty($jobData['result']['error']) ? StorageApiEvent::TYPE_SUCCESS : StorageApiEvent::TYPE_ERROR;
		$jobData['endTime'] = date('c');

		if (isset($jobData['result']['gdWriteStartTime'])) {
			$jobData['gdWriteStartTime'] = $jobData['result']['gdWriteStartTime'];
			unset($jobData['result']['gdWriteStartTime']);
		}
		$this->sharedConfig->saveJob($jobId, $jobData);

		$this->storageApiEvent->setDuration(time() - $startTime);
		$this->logEvent('end', $job, array('command' => $job['command'], 'params' => $logParams, 'result' => $jobData['result']));
	}


	/**
	 * Log event to client SAPI and to system log
	 */
	protected function logEvent($message, $job, $data = null)
	{
		$this->storageApiEvent
			->setMessage(sprintf('Job %d %s', $job['id'], $message))
			->setComponent($this->appConfiguration->appName)
			->setConfigurationId($job['writerId'])
			->setRunId($job['runId']);

		$this->storageApiClient->createEvent($this->storageApiEvent);

		$priority = $this->storageApiEvent->getType() == StorageApiEvent::TYPE_ERROR ? Logger::ERROR : Logger::INFO;
		$logData = array(
			'jobId' => $job['id'],
			'writerId' => $job['writerId'],
			'runId' => $job['runId']
		);
		if ($data) {
			$logData = array_merge($logData, $data);
		}
		$this->logger->log($priority, sprintf('Job %s %d', $message, $job['id']), $logData);
	}

}
