<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-02
 */

namespace Keboola\GoodDataWriter\Writer;

use Guzzle\Http\Exception\CurlException;
use Keboola\GoodDataWriter\Exception\JobProcessException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\StorageApi\ClientException as StorageApiClientException;
use Keboola\GoodDataWriter\GoodData\CsvHandlerNetworkException;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\Service\EventLogger;
use Keboola\GoodDataWriter\Service\Queue;
use Keboola\GoodDataWriter\Service\S3Client;
use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Event as StorageApiEvent,
	Keboola\StorageApi\Exception as StorageApiException;
use Monolog\Logger;
use Symfony\Component\Translation\TranslatorInterface;
use Syrup\ComponentBundle\Exception\UserException;
use Syrup\ComponentBundle\Filesystem\Temp;


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
	 * @var SharedStorage
	 */
	protected $sharedStorage;
	/**
	 * @var RestApi
	 */
	protected $restApi;
	/**
	 * @var Logger
	 */
	protected $logger;
	/**
	 * @var Temp
	 */
	protected $temp;

	/**
	 * @var StorageApiClient
	 */
	protected $storageApiClient;
	/**
	 * @var \Keboola\GoodDataWriter\Service\Queue
	 */
	protected $queue;
	/**
	 * @var \Symfony\Component\Translation\TranslatorInterface
	 */
	protected $translator;
	/**
	 * @var EventLogger
	 */
	protected $eventLogger;


	/**
	 *
	 */
	public function __construct(AppConfiguration $appConfiguration, SharedStorage $sharedStorage, RestApi $restApi,
								Logger $logger, Temp $temp, Queue $queue, TranslatorInterface $translator)
	{
		$this->appConfiguration = $appConfiguration;
		$this->sharedStorage = $sharedStorage;
		$this->restApi = $restApi;
		$this->logger = $logger;
		$this->temp = $temp;
		$this->queue = $queue;
		$this->translator = $translator;
	}

	/**
	 * Job execution
	 * Performs execution of job tasks and logging
	 */
	public function run($jobId, $forceRun = false)
	{
		$this->temp->initRunFolder();

		$startTime = time();

		$job = $this->sharedStorage->fetchJob($jobId);
		if (!$job) {
			throw new JobProcessException($this->translator->trans('job_executor.job_not_found %1', array('%1' => $jobId)));
		}

		// Job already executed?
		if (!$forceRun && $job['status'] != SharedStorage::JOB_STATUS_WAITING) {
			return;
		}

		$queueIdArray = explode('.', $job['queueId']);
		$serviceRun = isset($queueIdArray[2]) && $queueIdArray[2] == SharedStorage::SERVICE_QUEUE;

		$jobData = array('result' => array());
		try {
			$s3Client = new S3Client($this->appConfiguration, $job['projectId'] . '.' . $job['writerId'], $this->logger);

			$this->storageApiClient = new StorageApiClient(array(
				'token' => $job['token'],
				'url' => $this->appConfiguration->storageApiUrl,
				'userAgent' => $this->appConfiguration->userAgent
			));
			$this->storageApiClient->setRunId($jobId);
			$this->eventLogger = new EventLogger($this->appConfiguration, $this->storageApiClient, $s3Client);

			try {
				$configuration = new Configuration($this->storageApiClient, $this->sharedStorage);
				$configuration->setWriterId($job['writerId']);
				$writerInfo = $this->sharedStorage->getWriter($job['projectId'], $job['writerId']);
				if (!$serviceRun && $writerInfo['status'] == SharedStorage::WRITER_STATUS_MAINTENANCE) {
					throw new QueueUnavailableException($this->translator->trans('queue.maintenance'));
				}

				$this->sharedStorage->saveJob($jobId, array(
					'status' => SharedStorage::JOB_STATUS_PROCESSING,
					'startTime' => date('c', $startTime),
					'endTime' => null
				));
				$this->logEvent($this->translator->trans('log.job.started %1', array('%1' => $job['id'])), $job, array(
					'command' => $job['command'],
					'params' => $job['parameters']
				));

				$commandName = ucfirst($job['command']);
				$commandClass = 'Keboola\GoodDataWriter\Job\\' . $commandName;
				if (!class_exists($commandClass)) {
					throw new JobProcessException($this->translator->trans('job_executor.command_not_found %1', array('%1' => $commandName)));
				}

				$this->restApi->setJobId($job['id']);
				$this->restApi->setRunId($job['runId']);
				$this->restApi->setEventLogger($this->eventLogger);
				$bucketAttributes = $configuration->bucketAttributes();
				if (!empty($bucketAttributes['gd']['apiUrl'])) {
					$this->restApi->setBaseUrl($bucketAttributes['gd']['apiUrl']);
				}
				
				/**
				 * @var \Keboola\GoodDataWriter\Job\AbstractJob $command
				 */
				$command = new $commandClass($configuration, $this->appConfiguration, $this->sharedStorage, $s3Client,
					$this->translator, $this->storageApiClient, $this->eventLogger);
				$command->setTemp($this->temp); //For csv handler
				$command->setLogger($this->logger); //For csv handler
				$command->setQueue($this->queue);

				$error = null;

				try {
					$jobData['result'] = $command->run($job, $job['parameters'], $this->restApi);
				} catch (\Exception $e) {
					$debug = array(
						'message' => $e->getMessage(),
						'trace' => explode("\n", $e->getTraceAsString())
					);

					if ($e instanceof RestApiException) {
						$jobData['result']['error'] = $this->translator->trans('error.rest_api') . '. ' . $e->getMessage();
						$debug['details'] = $e->getDetails();
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
					} elseif ($e instanceof UserException) {
						$jobData['result']['error'] = $e->getMessage();
						$debug['details'] = $e->getData();
					} else {
						throw $e;
					}

					$jobData['debug'] = $s3Client->uploadString($job['id'] . '/debug-data.json', json_encode($debug, JSON_PRETTY_PRINT));
					$jobData['debug'] = $s3Client->url($jobData['debug']);
				}

				$jobData['logs'] = $command->getLogs();

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

		$this->sharedStorage->saveJob($jobId, $jobData);

		$log = array(
			'command' => $job['command'],
			'params' => $job['parameters'],
			'status' => $jobData['status'],
			'result' => $jobData['result']
		);
		if (isset($jobData['debug'])) $log['debug'] = $jobData['debug'];
		$this->logEvent($this->translator->trans('log.job.finished %1', array('%1' => $job['id'])), $job, $log, $startTime);
	}


	/**
	 * Log event to client SAPI and to system log
	 */
	protected function logEvent($message, $job, $data = null, $startTime = null)
	{
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

		$this->logger->log(Logger::INFO, $message, $logData);

		if (isset($data['debug']))
			unset($data['debug']);
		$this->eventLogger->log($job['id'], $job['runId'], $message, $data, time()-$startTime);
	}


	public function createJob($projectId, $writerId, $command, $params, $batchId, $queue=null, $others=array())
	{
		$jobData = array(
			'command' => $command,
			'batchId' => $batchId,
			'createdTime' => date('c'),
			'parameters' => $params,
			'queue' => $queue
		);
		if (count($others)) {
			$jobData = array_merge($jobData, $others);
		}

		$tokenData = $this->storageApiClient->getLogData();
		$job = $this->sharedStorage->createJob($this->storageApiClient->generateId(), $projectId, $writerId,
			$this->storageApiClient->getRunId(), $this->storageApiClient->token, $tokenData['id'],
			$tokenData['description'], $jobData);

		array_walk($params, function(&$val, $key) {
			if ($key == 'password') $val = '***';
		});
		$this->eventLogger->log($job['id'], $this->storageApiClient->getRunId(),
			$this->translator->trans($this->translator->trans('log.job.created')), array(
				'projectId' => $projectId,
				'writerId' => $writerId,
				'runId' => $this->storageApiClient->getRunId(),
				'command' => $command,
				'params' => $params
			));

		return $job;
	}

	public function addBatchToQueue($projectId, $writerId, $batchId)
	{
		$this->queue->enqueue(array(
			'projectId' => $projectId,
			'writerId' => $writerId,
			'batchId' => $batchId
		));
	}

	public function setStorageApiClient(StorageApiClient $storageApiClient)
	{
		$this->storageApiClient = $storageApiClient;
	}

	public function setEventLogger(EventLogger $eventLogger)
	{
		$this->eventLogger = $eventLogger;
	}

}
