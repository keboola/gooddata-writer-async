<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Event as StorageApiEvent,
	Keboola\StorageApi\Table as StorageApiTable,
	Keboola\GoodDataWriter\Service\S3Client;

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
	public function fetchJobs($projectId, $writerId, $days = 7)
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

		return StorageApiClient::parseCsv($csv, true);
	}

	/**
	 * @param $jobId
	 * @param null $writerId
	 * @param null $projectId
	 * @return mixed
	 */
	public function fetchJob($jobId, $writerId = null, $projectId = null)
	{
		$csv = $this->_storageApiClient->exportTable(
			self::JOBS_TABLE_ID,
			null,
			array(
				'whereColumn' => 'id',
				'whereValues' => array($jobId),
			)
		);

		$job = StorageApiClient::parseCsv($csv, true);
		$job = reset($job);

		if ((!$writerId || $job['writerId'] == $writerId) && (!$projectId || $job['projectId'] == $projectId)) {
			return $job;
		}

		return false;
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

	/**
	 * @param array $job
	 * @param S3Client $s3Client
	 * @return array
	 */
	public function jobToApiResponse(array $job, $s3Client = null)
	{
		if (!is_array($job['result'])) {
			$result = json_decode($job['result'], true);
			if (isset($result['debug']) && !is_array($result['debug'])) $result['debug'] = json_decode($result['debug'], true);
			if (isset($result['csvFile'])) unset($result['csvFile']);
			if ($result) {
				$job['result'] = $result;
			}
		}

		if (!is_array($job['parameters'])) {
			$params = json_decode($job['parameters'], true);
			if ($params) {
				$job['parameters'] = $params;
			}
		}

		// Find private links and make them accessible
		if ($s3Client) {
			if ($job['xmlFile']) {
				$url = parse_url($job['xmlFile']);
				if (empty($url['host'])) {
					$job['xmlFile'] = $s3Client->url($job['xmlFile'], 3600);
				}
			}
			if ($job['log']) {
				$url = parse_url($job['log']);
				if (empty($url['host'])) {
					$job['log'] = $s3Client->url($job['log'], 3600);
				}
			}
			if (!empty($job['result']['debug']) && is_array($job['result']['debug'])) {
				foreach ($job['result']['debug'] as $key => &$value) {
					$url = parse_url($value);
					if (empty($url['host'])) {
						$value = $s3Client->url($value, 3600);
					}
				}
			}
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
			'parameters' => $job['parameters'],
			'result' => $job['result'],
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
			'createdTime' => date('c'),
			'projectIdWriterId' => $job['projectId'] . '.' . $job['writerId']
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
			'createdTime' => date('c'),
			'projectIdWriterId' => $job['projectId'] . '.' . $job['writerId']
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
	 * @return mixed
	 */
	public function getProjects($projectId, $writerId)
	{
		$csv = $this->_storageApiClient->exportTable(
			self::PROJECTS_TABLE_ID,
			null,
			array(
				'whereColumn' => 'projectIdWriterId',
				'whereValues' => array($projectId . '.' . $writerId)
			)
		);

		return StorageApiClient::parseCsv($csv, true);
	}


	public function projectsToDelete()
	{
		$now = time();
		$csv = $this->_storageApiClient->exportTable(self::PROJECTS_TO_DELETE_TABLE_ID, null, array(
			'whereColumn' => 'deletedTime',
			'whereValues' => array('')
		));
		$result = array();
		foreach (StorageApiClient::parseCsv($csv) as $project) {
			if ($now - strtotime($project['createdTime']) >= 60 * 60 * 24 * 30) {
				$result[] = $project;
			}
		}
		return $result;
	}

	public function markProjectsDeleted($pids)
	{
		$nowTime = date('c');
		$data = array();
		foreach ($pids as $pid) {
			$data[] = array($pid, $nowTime);
		}

		$table = new StorageApiTable($this->_storageApiClient, self::PROJECTS_TO_DELETE_TABLE_ID, null, 'pid');
		$table->setHeader(array('pid', 'deletedTime'));
		$table->setFromArray($data);
		$table->setPartial(true);
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
			'createdTime' => date('c'),
			'deletedTime' => null,
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
	 * @return mixed
	 */
	public function getUsers($projectId, $writerId)
	{
		$csv = $this->_storageApiClient->exportTable(
			self::USERS_TABLE_ID,
			null,
			array(
				'whereColumn' => 'projectIdWriterId',
				'whereValues' => array($projectId . '.' . $writerId)
			)
		);

		return StorageApiClient::parseCsv($csv, true);
	}

	public function usersToDelete()
	{
		$now = time();
		$csv = $this->_storageApiClient->exportTable(self::USERS_TO_DELETE_TABLE_ID, null, array(
			'whereColumn' => 'deletedTime',
			'whereValues' => array('')
		));
		$result = array();
		foreach (StorageApiClient::parseCsv($csv) as $user) {
			if ($now - strtotime($user['createdTime']) >= 60 * 60 * 24 * 30) {
				$result[] = $user;
			}
		}
		return $result;
	}

	public function markUsersDeleted($ids)
	{
		$nowTime = date('c');
		$data = array();
		foreach ($ids as $id) {
			$data[] = array($id, $nowTime);
		}

		$table = new StorageApiTable($this->_storageApiClient, self::USERS_TO_DELETE_TABLE_ID, null, 'uid');
		$table->setHeader(array('uid', 'deletedTime'));
		$table->setFromArray($data);
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
			'createdTime' => date('c'),
			'deletedTime' => null,
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
