<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Writer;

use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Event as StorageApiEvent,
	Keboola\StorageApi\Table as StorageApiTable;

class SharedConfig
{
	const WRITER_NAME = 'gooddata_writer';
	const JOBS_TABLE_ID = 'in.c-wr-gooddata.jobs';
	const PROJECTS_TABLE_ID = 'in.c-wr-gooddata.projects';
	const USERS_TABLE_ID = 'in.c-wr-gooddata.users';
	const PROJECTS_TO_DELETE_TABLE_ID = 'in.c-wr-gooddata.projects_to_delete';
	const USERS_TO_DELETE_TABLE_ID = 'in.c-wr-gooddata.users_to_delete';


	/**
	 * @var StorageApiClient
	 */
	private $_storageApiClient;

	public function __construct($storageApiClient)
	{
		$this->_storageApiClient = $storageApiClient;
	}


	/**
	 * @param $projectId
	 * @param $writerId
	 * @param $days
	 * @return mixed
	 */
	public function fetchJobs($projectId, $writerId, $days)
	{
		$csv = $this->_storageApiClient->exportTable(
			self::JOBS_TABLE_ID,
			null,
			array(
				'whereColumn' => 'projectIdWriterId',
				'whereValues' => array($projectId . '.' . $writerId),
				'changedSince' => '-' . $days . ' days'
			)
		);

		$jobs = array();
		foreach (StorageApiClient::parseCsv($csv, true) as $j) {
			$jobs[] = $this->jobToApiResponse($j);
		}
		return $jobs;
	}

	/**
	 * @param $jobId
	 * @return mixed
	 */
	public function fetchJob($jobId)
	{
		$csv = $this->_storageApiClient->exportTable(
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

	/**
	 * @param $batchId
	 * @return mixed
	 */
	public function fetchBatch($batchId)
	{
		$csv = $this->_storageApiClient->exportTable(
			self::JOBS_TABLE_ID,
			null,
			array(
				'whereColumn' => 'batchId',
				'whereValues' => array($batchId),
			)
		);

		$jobs = StorageApiClient::parseCsv($csv, true);
		return $jobs;
	}

	/**
	 * @param $jobId
	 * @param $fields
	 */
	public function saveJob($jobId, $fields)
	{
		if (isset($fields['parameters'])) {
			$encodedParameters = json_encode($fields['parameters']);
			if ($encodedParameters) {
				$fields['parameters'] = $encodedParameters;
			}
		}
		if (isset($fields['result'])) {
			$encodedResult = json_encode($fields['result']);
			if ($encodedResult) {
				$fields['result'] = $encodedResult;
			}
		}

		$jobsTable = new StorageApiTable($this->_storageApiClient, self::JOBS_TABLE_ID);
		$jobsTable->setHeader(array_merge(array('id'), array_keys($fields)));
		$jobsTable->setFromArray(array(array_merge(array($jobId), $fields)));
		$jobsTable->setPartial(true);
		$jobsTable->setIncremental(true);
		$jobsTable->save();
	}

	public function jobToApiResponse(array $job)
	{
		if (!is_array($job['result'])) {
			$result = json_decode($job['result'], true);
			if (isset($result['debug']) && !is_array($result['debug'])) $result['debug'] = json_decode($result['debug']);
			if (isset($result['csvFile'])) unset($result['csvFile']);
			if (!$result) $result = $job['result'];
		} else {
			$result = $job['result'];
		}

		if (!is_array($job['parameters'])) {
			$params = json_decode($job['parameters'], true);
			if (!$params) $params = $job['parameters'];
		} else {
			$params = $job['parameters'];
		}

		return array(
			'id' => (int) $job['id'],
			'batchId' => (int) $job['batchId'],
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
			'dataset' => $job['dataset'],
			'xmlFile' => $job['xmlFile'],
			'parameters' => $params,
			'result' => $result,
			'gdWriteStartTime' => $job['gdWriteStartTime'],
			'gdWriteBytes' => $job['gdWriteBytes'] ? (int) $job['gdWriteBytes'] : null,
			'status' => $job['status'],
			'log' => $job['log'],
		);
	}



	public function saveProject($pid, $accessToken, $backendUrl, $job)
	{
		$data = array(
			'pid' => $pid,
			'projectId' => $job['projectId'],
			'writerId' => $job['writerId'],
			'backendUrl' => $backendUrl,
			'accessToken' => $accessToken,
			'createdTime' => date('c')
		);
		$table = new StorageApiTable($this->_storageApiClient, self::PROJECTS_TABLE_ID);
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setIncremental(true);
		$table->save();
	}

	public function saveUser($uid, $email, $job)
	{
		$data = array(
			'uid' => $uid,
			'projectId' => $job['projectId'],
			'writerId' => $job['writerId'],
			'email' => $email,
			'createdTime' => date('c')
		);
		$table = new StorageApiTable($this->_storageApiClient, self::USERS_TABLE_ID);
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setIncremental(true);
		$table->save();
	}


	/**
	 * @param $projectId
	 * @param $writerId
	 * @param $pid
	 * @param int $dev
	 */
	public function enqueueProjectToDelete($projectId, $writerId, $pid, $dev = 0)
	{
		$data = array(
			'pid' => $pid,
			'projectId' => $projectId,
			'writerId' => $writerId,
			'deleteDate' => date('c', strtotime('+30 days')),
			'dev' => $dev
		);
		$table = new StorageApiTable($this->_storageApiClient, self::PROJECTS_TO_DELETE_TABLE_ID, null, 'pid');
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();
	}

	/**
	 * @param $projectId
	 * @param $writerId
	 * @param $uid
	 * @param $email
	 * @param int $dev
	 */
	public function enqueueUserToDelete($projectId, $writerId, $uid, $email, $dev = 0)
	{
		$data = array(
			'uid' => $uid,
			'projectId' => $projectId,
			'writerId' => $writerId,
			'email' => $email,
			'deleteDate' => date('c', strtotime('+30 days')),
			'dev' => $dev
		);
		$table = new StorageApiTable($this->_storageApiClient, self::USERS_TO_DELETE_TABLE_ID, null, 'uid');
		$table->setHeader(array_keys($data));
		$table->setFromArray(array($data));
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();
	}


	public function logEvent($writerId, $runId, $message, $params = array(), $results = array())
	{
		$event = new StorageApiEvent();
		$event
			->setComponent(self::WRITER_NAME)
			->setConfigurationId($writerId)
			->setRunId($runId)
			->setParams($params)
			->setResults($results)
			->setMessage($message);
		$this->_storageApiClient->createEvent($event);
	}
}
