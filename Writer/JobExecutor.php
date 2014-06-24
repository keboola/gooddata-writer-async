<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-02
 */

namespace Keboola\GoodDataWriter\Writer;

use Guzzle\Http\Exception\CurlException;
use Keboola\GoodDataWriter\Exception\JobProcessException,
	Keboola\GoodDataWriter\Exception\ClientException,
	Keboola\GoodDataWriter\GoodData\CLToolApiErrorException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\StorageApi\ClientException as StorageApiClientException;
use Keboola\GoodDataWriter\GoodData\CsvHandlerException;
use Keboola\GoodDataWriter\GoodData\CsvHandlerNetworkException;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\Service\Queue;
use Keboola\GoodDataWriter\Service\S3Client;
use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Event as StorageApiEvent,
	Keboola\StorageApi\Exception as StorageApiException;
use Keboola\GoodDataWriter\Service\Lock;
use Monolog\Logger;
use Symfony\Component\Translation\TranslatorInterface;
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
	 * @var \Keboola\GoodDataWriter\Service\Queue
	 */
	protected $queue;
	/**
	 * @var \Symfony\Component\Translation\TranslatorInterface
	 */
	private $translator;


	/**
	 *
	 */
	public function __construct(AppConfiguration $appConfiguration, SharedConfig $sharedConfig, RestApi $restApi, Logger $logger,
								TempServiceFactory $tempServiceFactory, Queue $queue, TranslatorInterface $translator)
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
		$this->queue = $queue;
		$this->translator = $translator;

		$this->storageApiEvent = new StorageApiEvent();
	}

	public function runBatch($batchId, $forceRun = false)
	{
		$batch = $this->sharedConfig->batchToApiResponse($batchId);

		// Batch already executed?
		if (!$forceRun && $batch['status'] != SharedConfig::JOB_STATUS_WAITING) {
			return;
		}

		// Lock
		$pdo = new \PDO(sprintf('mysql:host=%s;dbname=%s', $this->appConfiguration->db_host, $this->appConfiguration->db_name),
			$this->appConfiguration->db_user, $this->appConfiguration->db_password);
		$pdo->exec('SET wait_timeout = 31536000;');
		$lock = new Lock($pdo, $batch['queueId']);
		if (!$lock->lock()) {
			throw new QueueUnavailableException($this->translator->trans('queue.in_use %1', array('%1' => $batchId)));
		}

		foreach ($batch['jobs'] as $job) {
			$this->runJob($job['id'], $forceRun);
		}

		$lock->unlock();
	}

	/**
	 * Job execution
	 * Performs execution of job tasks and logging
	 */
	public function runJob($jobId, $forceRun = false)
	{
		$startTime = time();

		$job = $this->sharedConfig->fetchJob($jobId);
		if (!$job) {
			throw new JobProcessException($this->translator->trans('job_executor.job_not_found %1', array('%1' => $jobId)));
		}

		// Job already executed?
		if (!$forceRun && $job['status'] != SharedConfig::JOB_STATUS_WAITING) {
			return;
		}

		$queueIdArray = explode('.', $job['queueId']);
		$serviceRun = isset($queueIdArray[2]) && $queueIdArray[2] == SharedConfig::SERVICE_QUEUE;

		$jobData = array('result' => array());
		$logParams = $job['parameters'];
		try {
			$this->storageApiClient = new StorageApiClient(array(
				'token' => $job['token'],
				'url' => $this->appConfiguration->storageApiUrl,
				'userAgent' => $this->appConfiguration->userAgent
			));
			$this->storageApiClient->setRunId($jobId);

			try {
				if ($job['parameters']) {
					$parameters = json_decode($job['parameters'], true);
					if ($parameters === false) {
						throw new JobProcessException($this->translator->trans('job_executor.bad_parameters'));
					}
				} else {
					$parameters = array();
				}

				$configuration = new Configuration($this->storageApiClient, $job['writerId'], $this->appConfiguration->scriptsPath);
				$bucketAttributes = $configuration->bucketAttributes();
				if (!$serviceRun && !empty($bucketAttributes['maintenance'])) {
					throw new QueueUnavailableException($this->translator->trans('queue.maintenance'));
				}

				$this->sharedConfig->saveJob($jobId, array(
					'status' => SharedConfig::JOB_STATUS_PROCESSING,
					'startTime' => date('c', $startTime),
					'endTime' => null
				));
				$logParams = $parameters;
				$this->logEvent($this->translator->trans('log.job.started %1', array('%1' => $job['id'])), $job, array(
					'command' => $job['command'],
					'params' => $logParams
				));

				$commandName = ucfirst($job['command']);
				$commandClass = 'Keboola\GoodDataWriter\Job\\' . $commandName;
				if (!class_exists($commandClass)) {
					throw new JobProcessException($this->translator->trans('job_executor.command_not_found %1', array('%1' => $commandName)));
				}

				$s3Client = new S3Client(
					$this->appConfiguration,
					$job['projectId'] . '.' . $job['writerId'],
					$this->logger
				);

				$this->restApi->setJobId($job['id']);
				$this->restApi->initLog();
				$token = $this->storageApiClient->getLogData();

				// make copy of $appConfiguration (otherwise next jobs processed by this worker will have following domain setup)
				$appConfiguration = clone $this->appConfiguration;
				if (!empty($token['owner']['features']) && in_array('gdwr-academy', $token['owner']['features'])) {
					$appConfiguration->gd_domain = 'keboola-academy';
				}

				$tempService = $this->tempServiceFactory->get('gooddata_writer');
				$preRelease = !empty($token['owner']['features']) && in_array('gdwr-prerelease', $token['owner']['features']);
				$isTesting = !empty($token['owner']['features']) && in_array('gdwr-testing', $token['owner']['features']);
				/**
				 * @var \Keboola\GoodDataWriter\Job\AbstractJob $command
				 */
				$command = new $commandClass($configuration, $appConfiguration, $this->sharedConfig, $this->restApi, $s3Client,
					$tempService, $this->translator, $this->storageApiClient, $job['id']);
				$command->setLogger($this->logger); //@TODO deprecated - only for CL tool
				$command->setQueue($this->queue);
				$command->setPreRelease($preRelease);
				$command->setIsTesting($isTesting);

				$error = null;

				try {
					$jobData['result'] = $command->run($job, $parameters);
				} catch (\Exception $e) {
					$debug = array(
						'message' => $e->getMessage(),
						'trace' => explode("\n", $e->getTraceAsString())
					);

					if ($e instanceof RestApiException) {
						$command->logEvent('restApi', array('error' => $e->getMessage()), $this->restApi->getLogPath());

						$jobData['result']['error'] = $this->translator->trans('error.rest_api') . '. ' . $e->getMessage();
						$debug['details'] = $e->getDetails();
					} elseif ($e instanceof CLToolApiErrorException) {
						$jobData['result']['error'] = $this->translator->trans('error.cl_tool') . '. ' . $e->getMessage();
						$debug['details'] = $e->getData();
					} elseif ($e instanceof StorageApiClientException) {
						$jobData['result']['error'] = $this->translator->trans('error.storage_api') . '. ' . $e->getMessage();
						if ($e->getPrevious() instanceof CurlException) {
							/* @var CurlException $curlException */
							$curlException = $e->getPrevious();
							$debug['curl'] = $curlException->getCurlInfo();
						}
					} elseif ($e instanceof CsvHandlerNetworkException) {
						$jobData['result']['error'] = $e->getMessage();
						$debug['details'] = $e->getData();
						$this->logger->alert($e->getMessage(), array(
							'job' => $job,
							'exception' => $e,
							'runId' => $this->storageApiClient->getRunId()
						));
					} elseif ($e instanceof ClientException) {
						$jobData['result']['error'] = $e->getMessage();
						$debug['details'] = $e->getData();
					} else {
						throw $e;
					}

					$jobData['debug'] = $s3Client->uploadString($job['id'] . '/debug-data.json', json_encode($debug, JSON_PRETTY_PRINT));
					$jobData['debug'] = $s3Client->url($jobData['debug']);
				}

				$apiLog = $s3Client->uploadFile($command->getLogPath(), 'text/plain', $job['id'] . '/log.json');

				$jobData['logs'] = $command->getLogs();
				$jobData['logs']['API Requests'] = $apiLog;

			} catch (\Exception $e) {
				if ($e instanceof QueueUnavailableException) {
					throw $e;
				} else {
					$this->logger->alert($e->getMessage(), array(
						'job' => $job,
						'exception' => $e,
						'runId' => $this->storageApiClient->getRunId()
					));
					$jobData['result']['error'] = $this->translator->trans('error.application');
				}
			}

		} catch(StorageApiException $e) {
			$jobData['result']['error'] = $this->translator->trans('error.storage_api') . ': ' . $e->getMessage();
		}

		$jobData['status'] = empty($jobData['result']['error']) ? StorageApiEvent::TYPE_SUCCESS : StorageApiEvent::TYPE_ERROR;
		$jobData['endTime'] = date('c');

		if (isset($jobData['result']['gdWriteStartTime'])) {
			$jobData['gdWriteStartTime'] = $jobData['result']['gdWriteStartTime'];
			unset($jobData['result']['gdWriteStartTime']);
		}
		$this->sharedConfig->saveJob($jobId, $jobData);

		$this->storageApiEvent->setDuration(time() - $startTime);
		$log = array(
			'command' => $job['command'],
			'params' => $logParams,
			'result' => $jobData['result']
		);
		if (isset($jobData['debug'])) $log['debug'] = $jobData['debug'];
		$this->logEvent($this->translator->trans('log.job.finished %1', array('%1' => $job['id'])), $job, $log);
	}


	/**
	 * Log event to client SAPI and to system log
	 */
	protected function logEvent($message, $job, $data = null)
	{
		$this->storageApiEvent
			->setMessage($message)
			->setComponent($this->appConfiguration->appName)
			->setConfigurationId($job['writerId'])
			->setRunId($job['runId']);

		$this->storageApiClient->createEvent($this->storageApiEvent);

		$priority = $this->storageApiEvent->getType() == StorageApiEvent::TYPE_ERROR ? Logger::ERROR : Logger::INFO;
		$logData = array(
			'jobId' => $job['id'],
			'projectId' => $job['projectId'],
			'writerId' => $job['writerId'],
			'runId' => $job['runId']
		);
		if ($data) {
			$logData = array_merge($logData, $data);
		}
		array_walk($logData['params'], function(&$val, $key) {
			if ($key == 'password') $val = '***';
		});
		$this->logger->log($priority, $message, $logData);
	}

}
