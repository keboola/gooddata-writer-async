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

	public function runBatch($batchId, $force = false)
	{
		$jobs = $this->_sharedConfig->fetchBatch($batchId);
		if (!count($jobs)) {
			throw new JobProcessException("Batch {$batchId} not found");
		}

		$batch = $this->_sharedConfig->batchToApiResponse($batchId);
		$gdWriterParams = $this->_container->getParameter('gooddata_writer');

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
		$job = $this->_job = $this->_sharedConfig->fetchJob($jobId);

		// Job not found?
		if (!$job) {
			throw new JobProcessException("Job $jobId not found");
		}

		// Job already executed?
		if (!$force && SharedConfig::isJobFinished($job['status'])) {
			return;
		}

		$gdWriterParams = $this->_container->getParameter('gooddata_writer');


		try {
			$this->_storageApiClient = new StorageApiClient(
				$job['token'],
				$this->_container->getParameter('storage_api.url'),
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
	}



	protected function _prepareSapiEventForJob($job)
	{
		$event = new StorageApiEvent();
		$event
			->setComponent($this->_container->getParameter('app_name'))
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

			$mainConfig = $this->_container->getParameter('gooddata_writer');
			$mainConfig['storage_api.url'] = $this->_container->getParameter('storage_api.url');

			$tmpDir = sprintf('%s/%s', $mainConfig['tmp_path'], $job['id']);
            if (!file_exists($mainConfig['tmp_path'])) mkdir($mainConfig['tmp_path']);
            if (!file_exists($tmpDir)) mkdir($tmpDir);

			// Do not migrate (migration had to be performed at least when the job was created)
			$configuration = new Configuration($this->_storageApiClient, $job['writerId'], false);

			$s3Client = new S3Client(
				\Aws\S3\S3Client::factory(array(
					'key' => $mainConfig['aws']['access_key'],
					'secret' => $mainConfig['aws']['secret_key'])
				),
				$mainConfig['aws']['s3_bucket'],
				$job['projectId'] . '.' . $job['writerId']
			);

			$restApi = new RestApi($this->_log);
			$bucketAttributes = $configuration->bucketAttributes();
			if (isset($bucketAttributes['gd']['backendUrl'])) {
				$restApi->setBaseUrl($bucketAttributes['gd']['backendUrl']);
			}

			/**
			 * @var \Keboola\GoodDataWriter\Job\AbstractJob $command
			 */
			$command = new $commandClass($configuration, $mainConfig, $this->_sharedConfig, $restApi, $s3Client);
			$command->tmpDir = $tmpDir;
			$command->scriptsPath = $mainConfig['scripts_path'];
			$command->log = $this->_log;
			try {
				$result = $command->run($job, $parameters);
			} catch (RestApiException $e) {
				$e2 = new ClientException('Rest API error: ' . $e->getMessage());
				$e2->setData(array('trace' => $e->getTraceAsString()));
				throw $e2;
			} catch (CLToolApiErrorException $e) {
				$e2 = new ClientException('CL Tool error: ' . $e->getMessage());
				$e2->setData(array('trace' => $e->getTraceAsString()));
				throw $e2;
			} catch (UnauthorizedException $e) {
				$e2 = new ClientException('Bad GoodData credentials: ' . $e->getMessage());
				$e2->setData(array('trace' => $e->getTraceAsString()));
				throw $e2;
			} catch (StorageApiClientException $e) {
				$e2 = new ClientException('Storage API problem: ' . $e->getMessage());
				$e2->setData(array('trace' => $e->getTraceAsString()));
				throw $e2;
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

			$data = $e->getData();
			if (count($data)) {
				$data['jobId'] = $job['id'];
				$data['runId'] = $this->_storageApiClient->getRunId();
				$this->_log->alert('Writer Error', $data);
			}

			return array('status' => 'error', 'error' => $e->getMessage());
		} catch (\Exception $e) {
			$duration = $time - time();

			$this->_log->alert('Job execution error', array(
				'jobId' => $job,
				'exception' => $e,
				'runId' => $this->_storageApiClient->getRunId()
			));

			$sapiEvent
				->setMessage("Job $job[id] end")
				->setType(StorageApiEvent::TYPE_WARN)
				->setDescription($e->getMessage())
				->setDuration($duration);
			$this->_logEvent($sapiEvent);

			return array('status' => 'error', 'error' => 'Application error');
		}
	}
}
