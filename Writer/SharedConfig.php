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


class SharedConfigException extends \Exception
{

}


class SharedConfig
{
	const WRITER_NAME = 'gooddata_writer';
	const JOBS_TABLE_ID = 'in.c-wr-gooddata.jobs';
	const PROJECTS_TABLE_ID = 'in.c-wr-gooddata.projects';
	const USERS_TABLE_ID = 'in.c-wr-gooddata.users';
	const PROJECTS_TO_DELETE_TABLE_ID = 'in.c-wr-gooddata.projects_to_delete';
	const USERS_TO_DELETE_TABLE_ID = 'in.c-wr-gooddata.users_to_delete';

	const JOB_STATUS_WAITING = 'waiting';
	const JOB_STATUS_PROCESSING = 'processing';
	const JOB_STATUS_SUCCESS = 'success';
	const JOB_STATUS_ERROR = 'error';
	const JOB_STATUS_CANCELLED = 'cancelled';

	/**
	 * @var StorageApiClient
	 */
	private $_storageApiClient;

	/**
	 * @param $storageApiClient
	 */
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
		return $this->_fetchTableRows(self::JOBS_TABLE_ID, 'projectIdWriterId', $projectId . '.' . $writerId, array(
			'changedSince' => '-' . $days . ' days'
		));
	}

	/**
	 * @param $jobId
	 * @param null $writerId
	 * @param null $projectId
	 * @return mixed
	 */
	public function fetchJob($jobId, $writerId = null, $projectId = null)
	{
		$job = $this->_fetchTableRows(self::JOBS_TABLE_ID, 'id', $jobId);
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
		return $this->_fetchTableRows(self::JOBS_TABLE_ID, 'batchId', $batchId);
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

		if (!isset($fields['id'])) {
			$fields['id'] = $jobId;
		}
		$this->_updateTableRow(self::JOBS_TABLE_ID, 'pid', $fields);
	}

	/**
	 * @param $status
	 * @return bool
	 */
	public static function isJobFinished($status)
	{
		return in_array($status, array(
			self::JOB_STATUS_SUCCESS,
			self::JOB_STATUS_ERROR,
			self::JOB_STATUS_CANCELLED
		));
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
					$job['xmlFile'] = $s3Client->url($job['xmlFile']);
				}
			}
			if ($job['log']) {
				$url = parse_url($job['log']);
				if (empty($url['host'])) {
					$job['log'] = $s3Client->url($job['log']);
				}
			}
			if (!empty($job['result']['debug']) && is_array($job['result']['debug'])) {
				foreach ($job['result']['debug'] as $key => &$value) {
					$url = parse_url($value);
					if (empty($url['host'])) {
						$value = $s3Client->url($value);
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

	/**
	 * @param $batchId
	 * @param null $s3Client
	 * @return array
	 * @throws SharedConfigException
	 */
	public function batchToApiResponse($batchId, $s3Client = null)
	{
		$data = array(
			'batchId' => (int)$batchId,
			'projectId' => null,
			'writerId' => null,
			'createdTime' => date('c'),
			'startTime' => date('c'),
			'endTime' => null,
			'status' => null,
			'jobs' => array(),
			'result' => null,
			'log' => null
		);
		$cancelledJobs = 0;
		$waitingJobs = 0;
		$processingJobs = 0;
		$errorJobs = 0;
		$successJobs = 0;
		foreach ($this->fetchBatch($batchId) as $job) {
			$job = $this->jobToApiResponse($job, $s3Client);

			if (!$data['projectId']) {
				$data['projectId'] = $job['projectId'];
			} elseif ($data['projectId'] != $job['projectId']) {
				throw new SharedConfigException(sprintf('ProjectId of job %s: %s does not match projectId %s of previous job.',
					$job['id'], $job['projectId'], $data['projectId']));
			}
			if (!$data['writerId']) {
				$data['writerId'] = $job['writerId'];
			} elseif ($data['writerId'] != $job['writerId']) {
				throw new SharedConfigException(sprintf('WriterId of job %s: %s does not match writerId %s of previous job.',
					$job['id'], $job['projectId'], $data['projectId']));
			}

			if ($job['createdTime'] < $data['createdTime']) $data['createdTime'] = $job['createdTime'];
			if ($job['startTime'] < $data['startTime']) $data['startTime'] = $job['startTime'];
			if ($job['endTime'] > $data['endTime']) $data['endTime'] = $job['endTime'];
			$data['jobs'][] = (int)$job['id'];
			if ($job['status'] == self::JOB_STATUS_WAITING) $waitingJobs++;
			elseif ($job['status'] == self::JOB_STATUS_PROCESSING) $processingJobs++;
			elseif ($job['status'] == self::JOB_STATUS_CANCELLED) $cancelledJobs++;
			elseif ($job['status'] == self::JOB_STATUS_ERROR) {
				$errorJobs++;
				$data['result'] = $job['result'];
			}
			else $successJobs++;
		}

		if ($cancelledJobs > 0) $data['status'] = self::JOB_STATUS_CANCELLED;
		elseif ($processingJobs > 0) $data['status'] = self::JOB_STATUS_PROCESSING;
		elseif ($waitingJobs > 0) $data['status'] = self::JOB_STATUS_WAITING;
		elseif ($errorJobs > 0) $data['status'] = self::JOB_STATUS_ERROR;
		else $data['status'] = self::JOB_STATUS_SUCCESS;

		return $data;
	}


	/**
	 * @param $pid
	 * @param $accessToken
	 * @param $backendUrl
	 * @param $job
	 */
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
		$this->_updateTableRow(self::PROJECTS_TABLE_ID, 'pid', $data);
	}

	/**
	 * @param $uid
	 * @param $email
	 * @param $job
	 */
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
		$this->_updateTableRow(self::USERS_TABLE_ID, 'uid', $data);
	}


	/**
	 * @param $projectId
	 * @param $writerId
	 * @return mixed
	 */
	public function getProjects($projectId, $writerId)
	{
		return $this->_fetchTableRows(self::PROJECTS_TABLE_ID, 'projectIdWriterId', $projectId . '.' . $writerId);
	}

	/**
	 * @return array
	 */
	public function projectsToDelete()
	{
		$now = time();
		$result = array();
		$csv = $this->_fetchTableRows(self::PROJECTS_TO_DELETE_TABLE_ID, 'deletedTime', '');
		foreach ($csv as $project) {
			if ($now - strtotime($project['createdTime']) >= 60 * 60 * 24 * 30) {
				$result[] = $project;
			}
		}
		return $result;
	}

	/**
	 * @param $pids
	 */
	public function markProjectsDeleted($pids)
	{
		$nowTime = date('c');
		$data = array();
		foreach ($pids as $pid) {
			$data[] = array($pid, $nowTime);
		}
		$this->_updateTable(self::PROJECTS_TO_DELETE_TABLE_ID, 'pid', array('pid', 'deletedTime'), $data);
	}

	/**
	 * @param $projectId
	 * @param $writerId
	 * @param $pid
	 * @param $backendUrl
	 * @param int $dev
	 */
	public function enqueueProjectToDelete($projectId, $writerId, $pid, $backendUrl = null, $dev = 0)
	{
		$data = array(
			'pid' => $pid,
			'projectId' => $projectId,
			'writerId' => $writerId,
			'backendUrl' => $backendUrl,
			'createdTime' => date('c'),
			'deletedTime' => null,
			'dev' => $dev
		);
		$this->_updateTableRow(self::PROJECTS_TO_DELETE_TABLE_ID, 'pid', $data);
	}


	/**
	 * @param $projectId
	 * @param $writerId
	 * @return mixed
	 */
	public function getUsers($projectId, $writerId)
	{
		return $this->_fetchTableRows(self::USERS_TABLE_ID, 'projectIdWriterId', $projectId . '.' . $writerId);
	}

	/**
	 * @return array
	 */
	public function usersToDelete()
	{
		$now = time();
		$result = array();
		$csv = $this->_fetchTableRows(self::USERS_TO_DELETE_TABLE_ID, 'deletedTime', '');
		foreach ($csv as $user) {
			if ($now - strtotime($user['createdTime']) >= 60 * 60 * 24 * 30) {
				$result[] = $user;
			}
		}
		return $result;
	}

	/**
	 * @param $ids
	 */
	public function markUsersDeleted($ids)
	{
		$nowTime = date('c');
		$data = array();
		foreach ($ids as $id) {
			$data[] = array($id, $nowTime);
		}

		$this->_updateTable(self::USERS_TO_DELETE_TABLE_ID, 'uid', array('uid', 'deletedTime'), $data);
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
		$this->_updateTableRow(self::USERS_TO_DELETE_TABLE_ID, 'uid', $data);
	}


	/**
	 * @param $writerId
	 * @param $runId
	 * @param $message
	 * @param array $params
	 * @param array $results
	 */
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


	/**
	 * @param $tableId
	 * @param $whereColumn
	 * @param $whereValue
	 * @param array $options
	 * @return array
	 */
	private function _fetchTableRows($tableId, $whereColumn, $whereValue, $options = array())
	{
		$exportOptions = array(
			'whereColumn' => $whereColumn,
			'whereValues' => array($whereValue)
		);
		if (count($options)) {
			$exportOptions = array_merge($exportOptions, $options);
		}
		$csv = $this->_storageApiClient->exportTable($tableId, null, $exportOptions);
		return StorageApiClient::parseCsv($csv, true);
	}

	/**
	 * @param $tableId
	 * @param $primaryKey
	 * @param $data
	 */
	private function _updateTableRow($tableId, $primaryKey, $data)
	{
		$this->_updateTable($tableId, $primaryKey, array_keys($data), array($data));
	}

	/**
	 * @param $tableId
	 * @param $primaryKey
	 * @param $headers
	 * @param $data
	 */
	private function _updateTable($tableId, $primaryKey, $headers, $data)
	{
		$table = new StorageApiTable($this->_storageApiClient, $tableId, null, $primaryKey);
		$table->setHeader($headers);
		$table->setFromArray($data);
		$table->setPartial(true);
		$table->setIncremental(true);
		$table->save();
	}
}
