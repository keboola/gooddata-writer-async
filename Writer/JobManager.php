<?php

namespace Keboola\GoodDataWriterBundle\Writer;

use Keboola\StorageApi\Event as StorageApiEvent,
	Keboola\StorageApi\Table as StorageApiTable;
use Monolog\Logger;

class JobManager
{
	const WRITER_NAME = 'wr-gooddata';
	const JOBS_TABLE_ID = 'in.c-wr-gooddata.jobs';


	public $configuration;
	/**
	 * @var Queue
	 */
	private $_queue;
	/**
	 * @var \Keboola\StorageApi\Client
	 */
	private $_storageApi;
	/**
	 * @var \Keboola\StorageApi\Client
	 */
	private $_sharedStorageApi;
	/**
	 * @var Logger
	 */
	private $_log;
	/**
	 * @var \Syrup\ComponentBundle\Monolog\Uploader\SyrupS3Uploader
	 */
	private $_logUploader;

	public function __construct($queue, $configuration, $storageApi, $sharedStorageApi, $log, $logUploader)
	{
		$this->configuration = $configuration;
		$this->_storageApi = $storageApi;
		$this->_sharedStorageApi = $sharedStorageApi;
		$this->_log = $log;
		$this->_logUploader = $logUploader;
		$this->_queue = $queue;
	}

	public function createJob($params)
	{
		$jobInfo = array(
			'runId' => $this->_storageApi->getRunId(),
			'projectId' => $this->configuration->projectId,
			'writerId' => $this->configuration->writerId,
			'tokenId' => $this->configuration->tokenInfo['id'],
			'tokenDesc' => $this->configuration->tokenInfo['description'],
			'tokenOwnerName' => $this->configuration->tokenInfo['owner']['name'],
			'initializedBy' => null,
			'createdTime' => null,
			'startTime' => null,
			'endTime' => null,
			'backendUrl' => $this->configuration->backendUrl,
			'pid' => null,
			'command' => null,
			'dataset' => null,
			'xmlFile' => null,
			'csvFile' => null,
			'parameters' => null,
			'result' => null,
			'gdWriteStartTime' => null,
			'gdWriteBytes' => null,
			'status' => 'waiting',
			'log' => null
		);
		$jobInfo = array_merge($jobInfo, $params);
		$jobId = $this->_storageApi->generateId();
		$this->_updateJobs($jobId, $jobInfo);

		$this->_queue->enqueueJob($params);

		// log event
		$event = new StorageApiEvent();
		$event
			->setComponent(self::WRITER_NAME)
			->setConfigurationId($this->configuration->writerId)
			->setRunId($jobInfo['runId'])
			->setParams($params)
			->setResults(array(
				'jobId' => $jobId,
			))
			->setMessage("Writer job $jobId created manually");
		$this->_logClientEvent($event);

		return $jobInfo;
	}

	public function finishJob($jobId, $status, $params, $logUrl)
	{
		$params = array_merge($params, array(
			'status' => $status,
			'log' => $logUrl
		));

		$this->_updateJobs($jobId, $params);

		$params = array_merge($params, array(
			'projectId' => $this->configuration->projectId,
			'writerId' => $this->configuration->writerId,
			'job' => $jobId,
			'runId' => $this->_storageApi->getRunId()
		));
		$logLevel = ($status == 'error') ? Logger::ERROR : Logger::INFO;
		$this->_log->log($logLevel, $params);
	}

	public function finishJobWithError($jobId, $command, $calls, $error)
	{
		$logUrl = $this->_logUploader->uploadString('calls-' . $jobId, $calls);
		$exceptionUrl = $this->_logUploader->uploadString('exception-' . $jobId, json_encode($error));
		$this->finishJob($jobId, 'error', array(
			'command' => $command,
			'result' => array('exception' => $exceptionUrl)
		), $logUrl);
	}

	protected function _updateJobs($jobId, $params)
	{
		$jobInfo = array_merge(array('id' => $jobId), $params);

		$table = new StorageApiTable($this->_sharedStorageApi, self::JOBS_TABLE_ID);
		$table->setHeader(array_keys($jobInfo));
		$table->setFromArray(array($jobInfo));
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();
	}



	protected function _logClientEvent(StorageApiEvent $event)
	{
		$this->_storageApi->createEvent($event);
		$this->_log->log($event->getMessage(), Logger::INFO, array(
			'token' => $this->_storageApi->getLogData(),
			'configurationId' => $event->getConfigurationId(),
			'runId' => $event->getRunId(),
			'description' => $event->getDescription(),
			'params' => $event->getParams(),
			'results' => $event->getResults(),
		));
	}

	protected function _jobToApiResponse(array $job)
	{
		try {
			$result = \Zend_Json::decode($job['result']);
		} catch (\Exception $e) {
			$result = $job['result'];
		}

		return array(
			'id' => (int) $job['id'],
			'runId' => (int) $job['id'],
			'projectId' => (int) $job['projectId'],
			'writerId' => (string) $job['writerId'],
			'token' => array(
				'id' => (int) $job['tokenId'],
				'description' => $job['tokenDesc'],
			),
			'initializedBy' => $job['initializedBy'],
			'createdTime' => $job['createdTime'],
			'startTime' => !empty($job['startTime']) ? $job['startTime'] : null,
			'endTime' => !empty($job['endTime']) ? $job['endTime'] : null,
			'command' => $job['command'],
			'pid' => $job['pid'],
			'dataset' => $job['dataset'],
			'xmlFile' => $job['xmlFile'],
			'csvFile' => $job['csvFile'],
			'parameters' => $job['parameters'],
			'result' => $result,
			'gdWriteStartTime' => $job['gdWriteStartTime'],
			'gdWriteBytes' => $job['gdWriteBytes'],
			'status' => $job['status'],
			'log' => $job['log'],
		);
	}
}