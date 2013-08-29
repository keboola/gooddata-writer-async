<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-02
 */

namespace Keboola\GoodDataWriter\Writer;

use Keboola\GoodDataWriter\Exception\JobProcessException;
use Keboola\GoodDataWriter\GoodData\CLToolApiErrorException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException,
	Keboola\GoodDataWriter\Exception\ClientException,
	Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Event as StorageApiEvent,
	Keboola\StorageApi\Table as StorageApiTable,
	Keboola\StorageApi\Exception as StorageApiException;
use Keboola\GoodDataWriter\Service\S3Client,
	Keboola\GoodDataWriter\GoodData\RestApi,
	Keboola\GoodDataWriter\GoodData\CLToolApi,
	Keboola\GoodDataWriter\Service\Lock;
use Monolog\Logger;
use Symfony\Component\DependencyInjection\ContainerInterface;


class JobCannotBeExecutedNowException extends \Exception
{

}


class JobExecutor
{
	const APP_NAME = 'gooddata-writer';


	/**
	 * @var SharedConfig
	 */
	protected $_sharedConfig;
	/**
	 * @var Logger
	 */
	protected $_log;
	/**
	 * Current job
	 * @var
	 */
	protected $_job = null;
	/**
	 * @var StorageApiClient
	 */
	protected $_storageApiClient = null;
	/**
	 * @var ContainerInterface
	 */
	protected $_container;


	/**
	 * @param SharedConfig $sharedConfig
	 * @param ContainerInterface $container
	 */
	public function __construct(SharedConfig $sharedConfig, ContainerInterface $container)
	{
		$this->_sharedConfig = $sharedConfig;
		$this->_log = $container->get('logger');
		$this->_container = $container;
	}

	public function runBatch($batchId)
	{
		$jobs = $this->_sharedConfig->fetchBatch($batchId);
		if (!count($jobs)) {
			throw new JobProcessException("Batch {$batchId} not found");
		}

		$batch = $this->_sharedConfig->batchToApiResponse($batchId);
		$gdWriterParams = $this->_container->getParameter('gooddata_writer');


		//@TODO Move to Job class
		$lockName = $batch['projectId'] . '-' . $batch['writerId'];

		$sqsClient = \Aws\Sqs\SqsClient::factory(array(
			'key' => $gdWriterParams['aws']['access_key'],
			'secret' => $gdWriterParams['aws']['secret_key'],
			'region' => $gdWriterParams['aws']['region']
		));
		$lock = new Lock(new \PDO(sprintf('mysql:host=%s;dbname=%s', $gdWriterParams['db']['host'], $gdWriterParams['db']['name']),
			$gdWriterParams['db']['user'], $gdWriterParams['db']['password']), $lockName);

		if (!$lock->lock()) {
			throw new JobCannotBeExecutedNowException("Batch {$batchId} cannot be executed now, another job already in progress on same writer.");
		}

		foreach ($jobs as $job) {
			$this->runJob($job['id']);
		}
	}

	/**
	 * Job execution
	 * Performs execution of job tasks and logging
	 * @param $jobId
	 * @throws JobCannotBeExecutedNowException
	 * @throws JobProcessException
	 */
	public function runJob($jobId)
	{
		$job = $this->_job = $this->_sharedConfig->fetchJob($jobId);

		// Job not found?
		if (!$job) {
			throw new JobProcessException("Job $jobId not found");
		}

		// Job already executed?
		if (SharedConfig::isJobFinished($job['status'])) {
			return;
		}

		$gdWriterParams = $this->_container->getParameter('gooddata_writer');


		try {
			$this->_storageApiClient = new StorageApiClient(
				$job['token'],
				$this->_container->getParameter('storageApi.url'),
				$gdWriterParams['user_agent']
			);
			$this->_storageApiClient->setRunId($jobId);

			// start work on job
			$this->_sharedConfig->saveJob($jobId, array(
				'status' => 'processing',
				'startTime' => date('c', time()),
			));

			$result = $this->_executeJob($job);

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
		$this->_sharedConfig->saveJob($jobId, $jobInfo);

		$lock->unlock();
	}



	protected function _prepareSapiEventForJob($job)
	{
		$event = new StorageApiEvent();
		$event
			->setComponent(self::APP_NAME)
			->setConfigurationId($job['writerId'])
			->setRunId($job['id']);

		return $event;
	}

	/**
	 * Log event to client SAPI and to system log
	 * @param StorageApiEvent $event
	 */
	protected function _logEvent(StorageApiEvent $event)
	{
		$event->setParams(array_merge($event->getParams(), array(
			'jobId' => $this->_job['id'],
			'writerId' => $this->_job['writerId']
		)));
		$this->_storageApiClient->createEvent($event);

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

		$this->_log($event->getMessage(), $priority, array(
			'writerId' => $event->getConfigurationId(),
			'runId' => $event->getRunId(),
			'description' => $event->getDescription(),
			'params' => $event->getParams(),
			'results' => $event->getResults(),
			'duration' => $event->getDuration(),
		));
	}

	protected function _log($message, $priority, array $data)
	{
		$this->_log->log($priority, $message, array_merge($data, array(
			'runId' => $this->_storageApiClient->getRunId(),
			'token' => $this->_storageApiClient->getLogData(),
			'jobId' => $this->_job['id'],
		)));
	}

	/**
	 * Excecute task and returns task execution result
	 * @param $job
	 * @throws WrongConfigurationException
	 * @return array
	 */
	protected function _executeJob($job)
	{
		$time = time();
		$sapiEvent = $this->_prepareSapiEventForJob($job);
		$sapiEvent->setMessage("Job $job[id] start");
		$this->_logEvent($sapiEvent);

		try {
			if ($job['parameters']) {
				$parameters = json_decode($job['parameters'], true);
				if (!$parameters) {
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

			$mainConfig = $this->_container->getParameter('gooddata_writer');
			$mainConfig['storageApi.url'] = $this->_container->getParameter('storageApi.url');

			$tmpDir = sprintf('%s/%s-%s', $mainConfig['tmp_path'], $job['id'], uniqid());
			if (!file_exists($tmpDir)) {
				mkdir($tmpDir);
			}

			$configuration = new Configuration($job['writerId'], $this->_storageApiClient, $tmpDir);

			$s3Client = new S3Client(
				\Aws\S3\S3Client::factory(array(
					'key' => $mainConfig['aws']['access_key'],
					'secret' => $mainConfig['aws']['secret_key'])
				),
				$mainConfig['aws']['s3_bucket'],
				$job['projectId'] . '.' . $job['writerId']
			);

			$backendUrl = isset($configuration->bucketInfo['gd']['backendUrl']) ? $configuration->bucketInfo['gd']['backendUrl'] : null;

			$restApi = new RestApi($backendUrl, $this->_log);

			$clToolApi = new CLToolApi($this->_log);
			$clToolApi->tmpDir = $tmpDir;
			$clToolApi->clToolPath = $mainConfig['cli_path'];
			$clToolApi->rootPath = $mainConfig['root_path'];
			$clToolApi->jobId = $job['id'];
			$clToolApi->s3client = $s3Client;
			if ($backendUrl) $clToolApi->setBackendUrl($backendUrl);

			/**
			 * @var \Keboola\GoodDataWriter\Job\GenericJob $command
			 */
			$command = new $commandClass($configuration, $mainConfig, $this->_sharedConfig, $restApi, $clToolApi, $s3Client);
			$command->tmpDir = $tmpDir;
			$command->rootPath = $mainConfig['root_path'];
			$command->log = $this->_log;
			try {
				$result = $command->run($job, $parameters);
			} catch (RestApiException $e) {
				throw new ClientException('Rest API error: ' . $e->getMessage());
			} catch (CLToolApiErrorException $e) {
				throw new ClientException('CL Tool error: ' . $e->getMessage());
			} catch (UnauthorizedException $e) {
				throw new ClientException('Bad GoodData credentials: ' . $e->getMessage());
			} catch (\Keboola\StorageApi\ClientException $e) {
				throw new ClientException('Storage API problem: ' . $e->getMessage());
			}

			$duration = time() - $time;
			$sapiEvent
				->setMessage("Job $job[id] end")
				->setDuration($duration);
			$this->_logEvent($sapiEvent);

			if (empty($result['status'])) $result['status'] = 'success';

			return $result;

		} catch (ClientException $e) {
			$duration = $time - time();

			$sapiEvent
				->setMessage("Job $job[id] end")
				->setType(StorageApiEvent::TYPE_WARN)
				->setDescription($e->getMessage())
				->setDuration($duration);
			$this->_logEvent($sapiEvent);

			return array('status' => 'error', 'error' => $e->getMessage());
		}
	}
}
