<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-02
 */

namespace Keboola\GoodDataWriter\Writer;

use Keboola\GoodDataWriter\GoodData\CLToolApiErrorException;
use Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Event as StorageApiEvent,
	Keboola\StorageApi\Table as StorageApiTable,
	Keboola\StorageApi\Exception as StorageApiException;
use Keboola\GoodDataWriter\Service\S3Client,
	Keboola\GoodDataWriter\GoodData\RestApi,
	Keboola\GoodDataWriter\GoodData\CLToolApi,
	Keboola\GoodDataWriter\Exception\ClientException,
	Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Monolog\Logger;
use Symfony\Component\DependencyInjection\ContainerInterface;

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

	/**
	 * Job execution
	 * Performs execution of job tasks and logging
	 * @param $jobId
	 * @throws WrongConfigurationException
	 */
	public function runJob($jobId)
	{
		$job = $this->_job = $this->_sharedConfig->fetchJob($jobId);

		if (!$job) {
			throw new WrongConfigurationException("Job $jobId not found");
		}

		try {
			$gdWriterParams = $this->_container->getParameter('gooddata_writer');
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
			$tmpDir = $mainConfig['tmp_path'];
			$configuration = new Configuration($job['writerId'], $this->_storageApiClient, $tmpDir);

			$s3Client = new S3Client($mainConfig['s3']['access_key'], $mainConfig['s3']['secret_key'],
				$mainConfig['s3']['bucket'], $job['projectId'] . '.' . $job['writerId']);

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
			$command->log = $this->_log;
			try {
				$result = $command->run($job, $parameters);
			} catch (RestApiException $e) {
				throw new ClientException('Rest API error: ' . $e->getMessage());
			} catch (CLToolApiErrorException $e) {
				throw new ClientException('CL Tool error: ' . $e->getMessage());
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
