<?php
/**
 * Job Manager
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @created 2013-04-02
 */

namespace Keboola\GoodDataWriterBundle\Writer;

use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Event as StorageApiEvent,
	Keboola\StorageApi\Table as StorageApiTable;
use Monolog\Logger;

class JobManager
{
	const WRITER_NAME = 'wr-gooddata';
	const JOBS_TABLE_ID = 'in.c-wr-gooddata.jobs';


	public $configuration;
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

	public function __construct($configuration, $storageApi, $sharedStorageApi, $log)
	{
		$this->configuration = $configuration;
		$this->_storageApi = $storageApi;
		$this->_sharedStorageApi = $sharedStorageApi;
		$this->_log = $log;
	}

	public function createJob($params)
	{
		$runId = $this->_storageApi->getRunId();
		$jobId = $this->_storageApi->generateId();
		$jobInfo = array(
			'id' => $jobId,
			'runId' => $runId,
			'projectId' => $this->configuration->projectId,
			'writerId' => $this->configuration->writerId,
			'sapiUrl' => $this->_storageApi->getApiUrl(),
			'token' => $this->_storageApi->token,
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
		$this->updateJob($jobId, $jobInfo);

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

	public function finishJob($jobId, $status, $params, $logUrl = null)
	{
		$params = array_merge($params, array(
			'status' => $status,
			'log' => $logUrl
		));

		$this->updateJob($jobId, $params);

		$params = array_merge($params, array(
			'projectId' => $this->configuration->projectId,
			'writerId' => $this->configuration->writerId,
			'job' => $jobId,
			'runId' => $this->_storageApi->getRunId()
		));
		$logLevel = ($status == 'error') ? Logger::ERROR : Logger::INFO;
		$this->_log->log($logLevel, $params);
	}

	public function finishJobWithError($jobId, $command, $logUrl = null, $error = null)
	{
		$data = array(
			'command' => $command
		);
		if ($error) $data['result'] = array('error' => $error);
		$this->finishJob($jobId, 'error', $data, $logUrl);
	}

	public function updateJob($jobId, $params)
	{
		if (isset($params['parameters'])) {
			$encodedParameters = json_encode($params['parameters']);
			if ($encodedParameters) {
				$params['parameters'] = $encodedParameters;
			}
		}
		if (isset($params['result'])) {
			$encodedResult = json_encode($params['result']);
			if ($encodedResult) {
				$params['result'] = $encodedResult;
			}
		}

		$jobInfo = array_merge(array('id' => $jobId), $params);

		$table = new StorageApiTable($this->_sharedStorageApi, self::JOBS_TABLE_ID);
		$table->setHeader(array_keys($jobInfo));
		$table->setFromArray(array($jobInfo));
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();
	}

	public function fetchJob($jobId)
	{
		$csv = $this->_sharedStorageApi->exportTable(
			self::JOBS_TABLE_ID,
			null,
			array(
				'whereColumn' => 'id',
				'whereValues' => array($jobId),
			)
		);

		$jobs = StorageApiClient::parseCsv($csv, true);
		return reset($jobs);
	}


	protected function _logClientEvent(StorageApiEvent $event)
	{
		$this->_storageApi->createEvent($event);
		$this->_log->log(Logger::INFO, $event->getMessage(), array(
			'token' => $this->_storageApi->getLogData(),
			'configurationId' => $event->getConfigurationId(),
			'runId' => $event->getRunId(),
			'description' => $event->getDescription(),
			'params' => $event->getParams(),
			'results' => $event->getResults(),
		));
	}

	public function jobToApiResponse(array $job)
	{
		try {
			$result = \Zend_Json::decode($job['result']);
		} catch (\Exception $e) {
			$result = $job['result'];
		}
		try {
			$params = \Zend_Json::decode($job['parameters']);
		} catch (\Exception $e) {
			$params = $job['parameters'];
		}

		return array(
			'id' => (int) $job['id'],
			'runId' => (int) $job['runId'],
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
			'parameters' => $params,
			'result' => $result,
			'gdWriteStartTime' => $job['gdWriteStartTime'],
			'gdWriteBytes' => $job['gdWriteBytes'],
			'status' => $job['status'],
			'log' => $job['log'],
		);
	}
}