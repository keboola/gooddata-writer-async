<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-02
 */

namespace Keboola\GoodDataWriter\Writer;

use Keboola\GoodDataWriter\Exception\JobProcessException,
	Keboola\GoodDataWriter\Exception\ClientException,
	Keboola\GoodDataWriter\Exception\WrongConfigurationException,
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


class JobCannotBeExecutedNowException extends \Exception
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
	 * @var StorageApiClient
	 */
	protected $storageApiClient = null;


	/**
	 * @param SharedConfig $sharedConfig
	 * @param ContainerInterface $container
	 */
	public function __construct(AppConfiguration $appConfiguration, SharedConfig $sharedConfig, RestApi $restApi, Logger $logger)
	{
		$this->appConfiguration = $appConfiguration;
		$this->sharedConfig = $sharedConfig;
		$this->restApi = $restApi;
		$this->logger = $logger;

		if (!defined('JSON_PRETTY_PRINT')) {
			// fallback for PHP <= 5.3
			define('JSON_PRETTY_PRINT', 0);
		}
	}

	public function runBatch($batchId, $force = false)
	{
		$jobs = $this->sharedConfig->fetchBatch($batchId);
		if (!count($jobs)) {
			throw new JobProcessException("Batch {$batchId} not found");
		}
		$batch = $this->sharedConfig->batchToApiResponse($batchId);

		// Batch already executed?
		if (!$force && SharedConfig::isJobFinished($batch['status'])) {
			return;
		}

		$lock = new Lock(new \PDO(sprintf('mysql:host=%s;dbname=%s', $this->appConfiguration->db_host, $this->appConfiguration->db_name),
			$this->appConfiguration->db_user, $this->appConfiguration->db_password), $batch['queueId']);

		if (!$lock->lock()) {
			throw new JobCannotBeExecutedNowException("Batch {$batchId} cannot be executed now, another job already in progress on same writer.");
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
		if (!$force && SharedConfig::isJobFinished($job['status'])) {
			return;
		}

		try {
			$this->storageApiClient = new StorageApiClient(
				$job['token'],
				$this->appConfiguration->storageApiUrl,
				$this->appConfiguration->userAgent
			);
			$this->storageApiClient->setRunId($jobId);

			// start work on job
			$this->sharedConfig->saveJob($jobId, array(
				'status' => 'processing',
				'startTime' => date('c', time()),
			));

			$time = time();
			$sapiEvent = $this->prepareSapiEventForJob($job);
			$sapiEvent->setMessage("Job $job[id] start");
			$this->logEvent($job, $sapiEvent);

			try {
				if ($job['parameters']) {
					$parameters = json_decode($job['parameters'], true);
					if ($parameters === false) {
						throw new WrongConfigurationException("Parameters decoding failed");
					}
				} else {
					$parameters = array();
				}

				$commandName = ucfirst($job['command']);
				$commandClass = 'Keboola\GoodDataWriter\Job\\' . $commandName;
				if (!class_exists($commandClass)) {
					throw new WrongConfigurationException(sprintf('Command %s does not exist', $commandName));
				}

				$tmpDir = sprintf('%s/%s', $this->appConfiguration->tmpPath, $job['id']);
				if (!file_exists($this->appConfiguration->tmpPath)) mkdir($this->appConfiguration->tmpPath);
				if (!file_exists($tmpDir)) mkdir($tmpDir);

				// Do not migrate (migration had to be performed at least when the job was created)
				$configuration = new Configuration($this->storageApiClient, $job['writerId'], $this->appConfiguration->scriptsPath, false);

				$s3Client = new S3Client(
					$this->appConfiguration,
					$job['projectId'] . '.' . $job['writerId']
				);

				$this->restApi->callsLog = array();
				$bucketAttributes = $configuration->bucketAttributes();
				if (isset($bucketAttributes['gd']['backendUrl'])) {
					$this->restApi->setBaseUrl($bucketAttributes['gd']['backendUrl']);
				}

				/**
				 * @var \Keboola\GoodDataWriter\Job\AbstractJob $command
				 */
				$command = new $commandClass($configuration, $this->appConfiguration, $this->sharedConfig, $this->restApi, $s3Client);
				$command->tmpDir = $tmpDir;
				$command->scriptsPath = $this->appConfiguration->scriptsPath;
				$command->log = $this->logger;

				$error = null;
				$token = $this->storageApiClient->getLogData();
				$command->preRelease = !empty($token['owner']['features']) && in_array('rc-writer', $token['owner']['features']);
				try {
					$result = $command->run($job, $parameters);
				} catch (\Exception $e) {
					$data = array();

					if ($e instanceof RestApiException) {
						$error = $e->getDetails();
					} elseif ($e instanceof CLToolApiErrorException) {
						$error = 'CL Tool';
						$data = $e->getData();
					} elseif ($e instanceof StorageApiClientException) {
						$error = 'Storage API';
					} elseif ($e instanceof ClientException) {
						$error = 'Error';
						$data = $e->getData();
					}

					if (count($data)) {
						$result['data'] = $s3Client->uploadString($job['id'] . '/debug-data.json', json_encode($data));
					}

					if ($error) {
						$result['status'] = 'error';
						$result['error'] = is_array($error) ? $error : array('error' => $error,  'details' => $e->getMessage());
						$result['trace'] = $s3Client->uploadString($job['id'] . '/trace.txt', json_encode($e->getTraceAsString(), JSON_PRETTY_PRINT));
					} else {
						throw $e;
					}
				}
				if (!empty($result['error'])) {
					if (isset($result['error']['error'])) {
						if (is_array($result['error']['error'])) {
							$result['error']['error'] = json_encode($result['error']['error']);
						}
						$sapiEvent->setDescription($result['error']['error']);
					}
					$sapiEvent->setType(StorageApiEvent::TYPE_WARN);
				}

				$logUrl = null;
				$log = count($command->eventsLog) ? $command->eventsLog : $this->restApi->callsLog();
				if (count($log)) {
					$result['log'] = $s3Client->uploadString($job['id'] . '/' . 'log.json', json_encode($log, JSON_PRETTY_PRINT));
				}


				$duration = time() - $time;
				$sapiEvent
					->setMessage("Job $job[id] end")
					->setDuration($duration)
					->setResults($result);
				$this->logEvent($job, $sapiEvent);
				if (empty($result['status'])) $result['status'] = 'success';

			} catch (\Exception $e) {
				$duration = $time - time();

				$this->logger->alert('Job execution error', array(
					'jobId' => $job,
					'exception' => $e,
					'runId' => $this->storageApiClient->getRunId()
				));

				$sapiEvent
					->setMessage("Job $job[id] end")
					->setType(StorageApiEvent::TYPE_WARN)
					->setDescription($e->getMessage())
					->setDuration($duration);
				$this->logEvent($job, $sapiEvent);

				$result = array('status' => 'error', 'error' => 'Application error');
			}

		} catch(StorageApiException $e) {
			$result = array('status' => 'error', 'error' => "Storage API error: " . $e->getMessage());
		}

		$jobStatus = ($result['status'] === 'error') ? StorageApiEvent::TYPE_ERROR : StorageApiEvent::TYPE_SUCCESS;

		// end work on job
		$jobInfo = array(
			'status' => $jobStatus,
			'endTime' => date('c'),
		);

		if (isset($result['gdWriteStartTime'])) {
			$jobInfo['gdWriteStartTime'] = $result['gdWriteStartTime'];
			unset($result['gdWriteStartTime']);
		}
		if (isset($result['gdWriteBytes'])) {
			$jobInfo['gdWriteBytes'] = $result['gdWriteBytes'];
			unset($result['gdWriteBytes']);
		}
		if (isset($result['log'])) {
			$jobInfo['log'] = $result['log'];
			unset($result['log']);
		}
		$jobInfo['result'] = $result;
		$this->sharedConfig->saveJob($jobId, $jobInfo);
	}



	protected function prepareSapiEventForJob($job)
	{
		$event = new StorageApiEvent();
		$event
			->setComponent($this->appConfiguration->appName)
			->setConfigurationId($job['writerId'])
			->setRunId($job['id']);

		return $event;
	}

	/**
	 * Log event to client SAPI and to system log
	 * @param StorageApiEvent $event
	 */
	protected function logEvent($job, StorageApiEvent $event)
	{
		$event->setParams(array_merge($event->getParams(), array(
			'jobId' => $job['id'],
			'writerId' => $job['writerId']
		)));
		$this->storageApiClient->createEvent($event);

		// convert priority
		switch ($event->getType()) {
			case StorageApiEvent::TYPE_ERROR:
				$priority = Logger::ERROR;
				break;
			case StorageApiEvent::TYPE_WARN:
				$priority = Logger::WARNING;
				break;
			default:
				$priority = Logger::INFO;
		}

		$logData = array(
			'jobId' => $job['id'],
			'writerId' => $event->getConfigurationId()
		);
		$description = $event->getDescription();
		if (!empty($description)) {
			$logData['description'] = $description;
		}
		$params = $event->getParams();
		if (count($params)) {
			$logData['params'] = $params;
		}
		$result = $event->getResults();
		if (count($result)) {
			$logData['result'] = $result;
		}
		$duration = $event->getDuration();
		if (!empty($duration)) {
			$logData['duration'] = $duration;
		}

		$this->logger->log($priority, $event->getMessage(), $logData);
	}

}
