<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Writer;

use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\GoodData\User;
use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\GoodDataWriter\Service\S3Client,
	Keboola\GoodDataWriter\Service\StorageApiConfiguration;
use Syrup\ComponentBundle\Service\Encryption\EncryptorFactory;


class SharedConfigException extends \Exception
{

}


class SharedConfig extends StorageApiConfiguration
{
	const WRITER_NAME = 'gooddata_writer';
	const DOMAINS_TABLE_ID = 'in.c-wr-gooddata.domains';
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

	const PRIMARY_QUEUE = 'primary';
	const SECONDARY_QUEUE = 'secondary';

	private $encryptor;


	public function __construct(AppConfiguration $appConfiguration, EncryptorFactory $encryptorFactory)
	{
		$this->_storageApiClient = new StorageApiClient(
			$appConfiguration->sharedSapi_token,
			$appConfiguration->sharedSapi_url,
			$appConfiguration->userAgent
		);
		$this->encryptor = $encryptorFactory->get('gooddata-writer'); //@TODO component name
	}

	public function getDomainUser($domain)
	{
		$result = $this->_fetchTableRows(self::DOMAINS_TABLE_ID, 'name', $domain);
		$result = reset($result);
		if (!$result || !isset($result['name']))
			throw new SharedConfigException(sprintf("User for domain '%s' does not exist", $domain));

		$user = new User();
		$user->domain = $result['name'];
		$user->username = $result['username'];
		$user->password = $this->encryptor->decrypt($result['password']);

		return $user;
	}

	public function saveDomain($name, $username, $password)
	{
		$this->_updateTableRow(self::DOMAINS_TABLE_ID, 'name', array(
			'name' => $name,
			'username' => $username,
			'password' => $this->encryptor->encrypt($password)
		));
	}


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
		$job = $this->_fetchTableRows(self::JOBS_TABLE_ID, 'id', $jobId, array(), false);
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
		return $this->_fetchTableRows(self::JOBS_TABLE_ID, 'batchId', $batchId, array(), false);
	}

	/**
	 * @param $projectId
	 * @param $writerId
	 */
	public function cancelJobs($projectId, $writerId)
	{
		foreach ($this->fetchJobs($projectId, $writerId) as $job) {
			if ($job['status'] == 'waiting') {
				$this->saveJob($job['id'], array('status' => 'cancelled'));
			}
		}
	}

	/**
	 * @param $jobId
	 * @param $fields
	 */
	public function saveJob($jobId, $fields)
	{
		$keysToEncode = array('parameters', 'result', 'logs', 'debug');
		foreach ($keysToEncode as $key) {
			if (isset($fields[$key])) {
				$encodedParameters = json_encode($fields[$key]);
				if ($encodedParameters) {
					$fields[$key] = $encodedParameters;
				}
			}
		}

		if (!isset($fields['id'])) {
			$fields['id'] = $jobId;
		}
		$this->_updateTableRow(self::JOBS_TABLE_ID, 'pid', $fields);
	}

	/**
	 * @param array $job
	 * @param S3Client $s3Client
	 * @return array
	 */
	public function jobToApiResponse(array $job, $s3Client = null)
	{
		$keysToDecode = array('parameters', 'result', 'logs', 'debug');
		foreach ($keysToDecode as $key) {
			if (!is_array($job[$key])) {
				$result = json_decode($job[$key], true);
				if (isset($result['debug']) && !is_array($result['debug']))
					$result['debug'] = json_decode($result['debug'], true);
				if ($result) {
					$job[$key] = $result;
				}
			}
		}

		if (isset($job['parameters']['accessToken'])) {
			$job['parameters']['accessToken'] = '***';
		}
		if (isset($job['parameters']['password'])) {
			$job['parameters']['password'] = '***';
		}

		$logs = array();
		if (is_array($job['logs'])) {
			$logs = $job['logs'];
		} else {
			if ($job['xmlFile']) {
				$logs['DataSet Definition'] = $job['xmlFile'];
			}
			if ($job['log']) {
				$logs['API Requests'] = $job['log'];
			}
			if (isset($job['result']['debug'])) {
				$logs = array_merge($logs, $job['result']['debug']);
			}
		}

		if (!empty($job['definition'])) {
			$logs['DataSet Definition'] = $job['definition'];
		}

		// Find private links and make them accessible
		if ($s3Client) {
			foreach ($logs as $key => &$log) {
				if (is_array($log)) foreach ($log as $k => &$v) {
					$url = parse_url($v);
					if (empty($url['host'])) {
						$v = $s3Client->url($v);
					}
				} else {
					$url = parse_url($log);
					if (empty($url['host'])) {
						$log = $s3Client->url($log);
					}
				}
			}
		}

		$result = array(
			'id' => (int) $job['id'],
			'batchId' => (int) $job['batchId'],
			'runId' => (int) $job['runId'],
			'projectId' => (int) $job['projectId'],
			'writerId' => (string) $job['writerId'],
			'queueId' => !empty($job['queueId']) ? $job['queueId'] : sprintf('%s.%s.%s', $job['projectId'], $job['writerId'], self::PRIMARY_QUEUE),
			'token' => array(
				'id' => (int) $job['tokenId'],
				'description' => $job['tokenDesc'],
			),
			'createdTime' => $job['createdTime'],
			'startTime' => !empty($job['startTime']) ? $job['startTime'] : null,
			'endTime' => !empty($job['endTime']) ? $job['endTime'] : null,
			'command' => $job['command'],
			'dataset' => $job['dataset'],
			'parameters' => $job['parameters'],
			'result' => $job['result'],
			'gdWriteStartTime' => $job['gdWriteStartTime'],
			'status' => $job['status'],
			'logs' => $logs
		);

		//@TODO REMOVE
		$result['log'] = isset($logs['API Requests'])? $logs['API Requests'] : null;
		if (isset($result['result']['debug'])) {
			$result['result']['debug'] = array_diff_key($result['logs'], array('DataSet Definition' => null, 'API Requests' => null));
		}
		//@TODO REMOVE

		return $result;
	}

	/**
	 * @param $batchId
	 * @param null $s3Client
	 * @return array
	 * @throws SharedConfigException
	 */
	public function batchToApiResponse($batchId, $s3Client = null)
	{
		$jobs = $this->fetchBatch($batchId);
		if (!count($jobs)) {
			throw new WrongParametersException(sprintf("Batch '%d' not found", $batchId));
		}

		$data = array(
			'batchId' => (int)$batchId,
			'projectId' => null,
			'writerId' => null,
			'queueId' => null,
			'createdTime' => date('c'),
			'startTime' => date('c'),
			'endTime' => null,
			'status' => null,
			'jobs' => array()
		);
		$cancelledJobs = 0;
		$waitingJobs = 0;
		$processingJobs = 0;
		$errorJobs = 0;
		$successJobs = 0;
		foreach ($jobs as $job) {
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

			if ($job['queueId'] && $job['queueId'] != self::PRIMARY_QUEUE) {
				$data['queueId'] = $job['queueId'];
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

		if (!$data['queueId']) $data['queueId'] = sprintf('%s.%s.%s', $data['projectId'], $data['writerId'], self::PRIMARY_QUEUE);

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


	public function enqueueProjectToDelete($projectId, $writerId, $pid)
	{
		$data = array(
			'pid' => $pid,
			'projectId' => $projectId,
			'writerId' => $writerId,
			'createdTime' => date('c'),
			'deletedTime' => null
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
	 */
	public function enqueueUserToDelete($projectId, $writerId, $uid, $email)
	{
		$data = array(
			'uid' => $uid,
			'projectId' => $projectId,
			'writerId' => $writerId,
			'email' => $email,
			'createdTime' => date('c'),
			'deletedTime' => null
		);
		$this->_updateTableRow(self::USERS_TO_DELETE_TABLE_ID, 'uid', $data);
	}

}
