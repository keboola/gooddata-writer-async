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
	Keboola\GoodDataWriter\GoodData\UnauthorizedException,
	Keboola\StorageApi\ClientException as StorageApiClientException;
use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Event as StorageApiEvent,
	Keboola\StorageApi\Exception as StorageApiException;
use Keboola\GoodDataWriter\Service\S3Client,
	Keboola\GoodDataWriter\GoodData\RestApi,
	Keboola\GoodDataWriter\Service\Lock;
use Monolog\Logger;
use Symfony\Component\DependencyInjection\ContainerInterface;


class JobCannotBeExecutedNowException extends \Exception
{

}


class JobExecutor
{

	/**
	 * @var SharedConfig
	 */
	protected $sharedConfig;
	/**
	 * @var Logger
	 */
	protected $log;
	/**
	 * Current job
	 * @var
	 */
	protected $job = null;
	/**
	 * @var StorageApiClient
	 */
	protected $storageApiClient = null;
	/**
	 * @var ContainerInterface
	 */
	protected $container;


	/**
	 * @param SharedConfig $sharedConfig
	 * @param ContainerInterface $container
	 */
	public function __construct(SharedConfig $sharedConfig, ContainerInterface $container)
	{
		$this->sharedConfig = $sharedConfig;
		$this->log = $container->get('logger');
		$this->container = $container;
	}

	public function runBatch($batchId, $force = false)
	{
		$jobs = $this->sharedConfig->fetchBatch($batchId);
		if (!count($jobs)) {
			throw new JobProcessException("Batch {$batchId} not found");
		}

		$batch = $this->sharedConfig->batchToApiResponse($batchId);
		$gdWriterParams = $this->container->getParameter('gooddata_writer');

		// Batch already executed?
		if (!$force && SharedConfig::isJobFinished($batch['status'])) {
			return;
		}

		$lock = new Lock(new \PDO(sprintf('mysql:host=%s;dbname=%s', $gdWriterParams['db']['host'], $gdWriterParams['db']['name']),
			$gdWriterParams['db']['user'], $gdWriterParams['db']['password']), $batch['queueId']);

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
		$job = $this->job = $this->sharedConfig->fetchJob($jobId);

		// Job not found?
		if (!$job) {
			throw new JobProcessException("Job $jobId not found");
		}

		// Job already executed?
		if (!$force && SharedConfig::isJobFinished($job['status'])) {
			return;
		}

		$gdWriterParams = $this->container->getParameter('gooddata_writer');


		try {
			$this->storageApiClient = new StorageApiClient(
				$job['token'],
				$this->container->getParameter('storage_api.url'),
				$gdWriterParams['user_agent']
			);
			$this->storageApiClient->setRunId($jobId);

			// start work on job
			$this->sharedConfig->saveJob($jobId, array(
				'status' => 'processing',
				'startTime' => date('c', time()),
			));

			$result = $this->executeJob($job);

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
			->setComponent($this->container->getParameter('app_name'))
			->setConfigurationId($job['writerId'])
			->setRunId($job['id']);

		return $event;
	}

	/**
	 * Log event to client SAPI and to system log
	 * @param StorageApiEvent $event
	 */
	protected function logEvent(StorageApiEvent $event)
	{
		$event->setParams(array_merge($event->getParams(), array(
			'jobId' => $this->job['id'],
			'writerId' => $this->job['writerId']
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
			'jobId' => $this->job['id'],
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

		$this->log->log($priority, $event->getMessage(), $logData);
	}

	/**
	 * Excecute task and returns task execution result
	 * @param $job
	 * @throws WrongConfigurationException
	 * @return array
	 */
	protected function executeJob($job)
	{
		$time = time();
		$sapiEvent = $this->prepareSapiEventForJob($job);
		$sapiEvent->setMessage("Job $job[id] start");
		$this->logEvent($sapiEvent);

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

			$mainConfig = $this->container->getParameter('gooddata_writer');
			$mainConfig['storage_api.url'] = $this->container->getParameter('storage_api.url');

			$tmpDir = sprintf('%s/%s', $mainConfig['tmp_path'], $job['id']);
            if (!file_exists($mainConfig['tmp_path'])) mkdir($mainConfig['tmp_path']);
            if (!file_exists($tmpDir)) mkdir($tmpDir);

			// Do not migrate (migration had to be performed at least when the job was created)
			$configuration = new Configuration($this->storageApiClient, $job['writerId'], $mainConfig['scripts_path'], false);

			$s3Client = new S3Client(
				\Aws\S3\S3Client::factory(array(
					'key' => $mainConfig['aws']['access_key'],
					'secret' => $mainConfig['aws']['secret_key'])
				),
				$mainConfig['aws']['s3_bucket'],
				$job['projectId'] . '.' . $job['writerId']
			);

			$restApi = new RestApi($this->log, $mainConfig['scripts_path']);
			$bucketAttributes = $configuration->bucketAttributes();
			if (isset($bucketAttributes['gd']['backendUrl'])) {
				$restApi->setBaseUrl($bucketAttributes['gd']['backendUrl']);
			}

			/**
			 * @var \Keboola\GoodDataWriter\Job\AbstractJob $command
			 */
			$command = new $commandClass($configuration, $mainConfig, $this->sharedConfig, $restApi, $s3Client);
			$command->tmpDir = $tmpDir;
			$command->scriptsPath = $mainConfig['scripts_path'];
			$command->log = $this->log;

			$token = $this->storageApiClient->getLogData();
			$command->preRelease = !empty($token['owner']['features']) && in_array('rc-writer', $token['owner']['features']);
			try {
				$result = $command->run($job, $parameters);
			} catch (\Exception $e) {
				$error = null;
				$data = array();

				if ($e instanceof RestApiException) {
					$error = 'Rest API';
				} elseif ($e instanceof CLToolApiErrorException) {
					$error = 'CL Tool';
					$data = $e->getData();
				} elseif ($e instanceof UnauthorizedException) {
					$error = 'Bad GoodData credentials';
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
					$result['error'] = $error . ': ' . $e->getMessage();
					$result['trace'] = $s3Client->uploadString($job['id'] . '/trace.txt', json_encode($e->getTraceAsString(), JSON_PRETTY_PRINT));
				} else {
					throw $e;
				}
			}
			if (!empty($result['error'])) {
				$sapiEvent->setDescription($result['error']);
				$sapiEvent->setType(StorageApiEvent::TYPE_WARN);
			}

			$duration = time() - $time;
			$sapiEvent
				->setMessage("Job $job[id] end")
				->setDuration($duration)
				->setResults($result);
			$this->logEvent($sapiEvent);
			if (empty($result['status'])) $result['status'] = 'success';

			return $result;

		} catch (\Exception $e) {
			$duration = $time - time();

			$this->log->alert('Job execution error', array(
				'jobId' => $job,
				'exception' => $e,
				'runId' => $this->storageApiClient->getRunId()
			));

			$sapiEvent
				->setMessage("Job $job[id] end")
				->setType(StorageApiEvent::TYPE_WARN)
				->setDescription($e->getMessage())
				->setDuration($duration);
			$this->logEvent($sapiEvent);

			return array('status' => 'error', 'error' => 'Application error');
		}
	}
}
