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
	const INVITATIONS_TABLE_ID = 'in.c-wr-gooddata.invitations';

	const JOB_STATUS_WAITING = 'waiting';
	const JOB_STATUS_PROCESSING = 'processing';
	const JOB_STATUS_SUCCESS = 'success';
	const JOB_STATUS_ERROR = 'error';
	const JOB_STATUS_CANCELLED = 'cancelled';

	const PRIMARY_QUEUE = 'primary';
	const SECONDARY_QUEUE = 'secondary';
	const SERVICE_QUEUE = 'service';

	private $encryptor;


	public function __construct(AppConfiguration $appConfiguration, EncryptorFactory $encryptorFactory)
	{
		$this->storageApiClient = new StorageApiClient(array(
			'token' => $appConfiguration->sharedSapi_token,
			'url' => $appConfiguration->sharedSapi_url,
			'userAgent' => $appConfiguration->userAgent
		));
		$this->encryptor = $encryptorFactory->get('gooddata-writer'); //@TODO $appConfiguration->appName // will need to re-encrypt passwords in testing environments
	}

	public static function isJobFinished($status)
	{
		return !in_array($status, array(SharedConfig::JOB_STATUS_WAITING, SharedConfig::JOB_STATUS_PROCESSING));
	}

	public function getDomainUser($domain)
	{
		$result = $this->fetchTableRows(self::DOMAINS_TABLE_ID, 'name', $domain);
		$result = reset($result);
		if (!$result || !isset($result['name']))
			throw new SharedConfigException(sprintf("User for domain '%s' does not exist", $domain));

		$user = new User();
		$user->domain = $result['name'];
		$user->username = $result['username'];
		$user->password = $this->encryptor->decrypt($result['password']);
		$user->uid = $result['uid'];

		return $user;
	}

	public function saveDomain($name, $username, $password)
	{
		$this->updateTableRow(self::DOMAINS_TABLE_ID, 'name', array(
			'name' => $name,
			'username' => $username,
			'password' => $this->encryptor->encrypt($password)
		));
	}


	public function fetchJobs($projectId, $writerId, $days = 7)
	{
		return $this->fetchTableRows(self::JOBS_TABLE_ID, 'projectIdWriterId', $projectId . '.' . $writerId, array(
			'changedSince' => '-' . $days . ' days'
		), false);
	}

	/**
	 * @param $jobId
	 * @param null $writerId
	 * @param null $projectId
	 * @return mixed
	 */
	public function fetchJob($jobId, $writerId = null, $projectId = null)
	{
		$job = $this->fetchTableRows(self::JOBS_TABLE_ID, 'id', $jobId, array(), false);
		$job = reset($job);

		if ((!$writerId || $job['writerId'] == $writerId) && (!$projectId || $job['projectId'] == $projectId)) {
			return $job;
		}

		return false;
	}

	/**
	 *
	 */
	public function fetchBatch($batchId)
	{
		return $this->fetchTableRows(self::JOBS_TABLE_ID, 'batchId', $batchId, array(), false);
	}

	/**
	 *
	 */
	public function cancelJobs($projectId, $writerId)
	{
		foreach ($this->fetchJobs($projectId, $writerId) as $job) {
			if ($job['status'] == self::JOB_STATUS_WAITING) {
				$this->saveJob($job['id'], array('status' => self::JOB_STATUS_CANCELLED));
			}
		}
	}

	/**
	 * Create new job
	 */
	public function createJob($projectId, $writerId, $runId, $token, $tokenId, $tokenOwner, $params)
	{
		$jobId = $this->storageApiClient->generateId();
		if (!isset($params['batchId'])) {
			$params['batchId'] = $jobId;
		}

		$jobInfo = array(
			'id' => $jobId,
			'runId' => $runId,
			'projectId' => $projectId,
			'writerId' => $writerId,
			'token' => $token,
			'tokenId' => $tokenId,
			'tokenDesc' => $tokenOwner,
			'createdTime' => null,
			'startTime' => null,
			'gdWriteStartTime' => null,
			'endTime' => null,
			'command' => null,
			'dataset' => null,
			'parameters' => null,
			'result' => null,
			'status' => self::JOB_STATUS_WAITING,
			'logs' => null,
			'debug' => null,
			'projectIdWriterId' => sprintf('%s.%s', $projectId, $writerId),
			'queueId' => sprintf('%s.%s.%s', $projectId, $writerId, isset($params['queue']) ? $params['queue'] : self::PRIMARY_QUEUE)
		);
		unset($params['queue']);
		$jobInfo = array_merge($jobInfo, $params);

		$this->saveJob($jobId, $jobInfo);
		return $jobInfo;
	}

	/**
	 * Update existing job
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
		$this->updateTableRow(self::JOBS_TABLE_ID, 'id', $fields);
	}

	/**
	 *
	 */
	public function jobToApiResponse(array $job, S3Client $s3Client = null)
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

		$logs = is_array($job['logs']) ? $job['logs'] : array();
		if (!empty($job['definition'])) {
			$logs['DataSet Definition'] = $job['definition'];
		}

		// Find private links and make them accessible
		if ($s3Client) {
			foreach ($logs as &$log) {
				if (is_array($log)) foreach ($log as &$v) {
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

		return $result;
	}

	/**
	 *
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
				$data['result'][$job['id']] = $job['result'];
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
	 * Save project to shared config
	 */
	public function saveProject($projectId, $writerId, $pid, $accessToken=null, $keepAfterRemoval = false)
	{
		$data = array(
			'pid' => $pid,
			'projectId' => $projectId,
			'writerId' => $writerId,
			'accessToken' => $accessToken,
			'createdTime' => date('c'),
			'projectIdWriterId' => $projectId . '.' . $writerId,
			'keepAfterRemoval' => $keepAfterRemoval
		);
		$this->updateTableRow(self::PROJECTS_TABLE_ID, 'pid', $data);
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
			'email' => strtolower($email),
			'createdTime' => date('c'),
			'projectIdWriterId' => $job['projectId'] . '.' . $job['writerId']
		);
		$this->updateTableRow(self::USERS_TABLE_ID, 'uid', $data);
	}


	/**
	 * @param $projectId
	 * @param $writerId
	 * @return mixed
	 */
	public function getProjects($projectId = null, $writerId = null)
	{
		if ($projectId && $writerId) {
			return $this->fetchTableRows(self::PROJECTS_TABLE_ID, 'projectIdWriterId', $projectId . '.' . $writerId);
		} else {
			return $this->fetchTableRows(self::PROJECTS_TABLE_ID);
		}
	}

	public function projectBelongsToWriter($projectId, $writerId, $pid)
	{
		foreach ($this->fetchTableRows(self::PROJECTS_TABLE_ID, 'pid', $pid) as $p) {
			if ($p['projectId'] == $projectId && $p['writerId'] == $writerId)
				return true;
		}
		return false;
	}

	/**
	 * @return array
	 */
	public function projectsToDelete()
	{
		$now = time();
		$result = array();
		$csv = $this->fetchTableRows(self::PROJECTS_TO_DELETE_TABLE_ID, 'deletedTime', '');
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
		$this->updateTable(self::PROJECTS_TO_DELETE_TABLE_ID, 'pid', array('pid', 'deletedTime'), $data);
	}


	public function enqueueProjectToDelete($projectId, $writerId, $pid)
	{
		$this->deleteTableRow(self::PROJECTS_TABLE_ID, 'pid', $pid);
		$this->updateTableRow(self::PROJECTS_TO_DELETE_TABLE_ID, 'pid', array(
			'pid' => $pid,
			'projectId' => $projectId,
			'writerId' => $writerId,
			'createdTime' => date('c'),
			'deletedTime' => null
		));
	}


	/**
	 * @param $projectId
	 * @param $writerId
	 * @return mixed
	 */
	public function getUsers($projectId, $writerId)
	{
		return $this->fetchTableRows(self::USERS_TABLE_ID, 'projectIdWriterId', $projectId . '.' . $writerId);
	}

	public function userBelongsToWriter($projectId, $writerId, $email)
	{
		foreach ($this->fetchTableRows(self::USERS_TABLE_ID, 'projectIdWriterId', $projectId.'.'.$writerId) as $u) {
			if (strtolower($u['email']) == strtolower($email))
				return true;
		}
		return false;
	}

	/**
	 * @return array
	 */
	public function usersToDelete()
	{
		$now = time();
		$result = array();
		$csv = $this->fetchTableRows(self::USERS_TO_DELETE_TABLE_ID, 'deletedTime', '');
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

		$this->updateTable(self::USERS_TO_DELETE_TABLE_ID, 'uid', array('uid', 'deletedTime'), $data);
	}

	/**
	 * @param $projectId
	 * @param $writerId
	 * @param $uid
	 * @param $email
	 */
	public function enqueueUserToDelete($projectId, $writerId, $uid, $email)
	{
		$this->deleteTableRow(self::USERS_TABLE_ID, 'uid', $uid);
		$this->updateTableRow(self::USERS_TO_DELETE_TABLE_ID, 'uid', array(
			'uid' => $uid,
			'projectId' => $projectId,
			'writerId' => $writerId,
			'email' => strtolower($email),
			'createdTime' => date('c'),
			'deletedTime' => null
		));
	}


	public function logInvitation($data)
	{
		$data = array(
			'pid' => $data['pid'],
			'sender' => $data['sender'],
			'createdTime' => $data['createDate'],
			'acceptedTime' => date('c'),
			'status' => $data['status'],
			'error' => isset($data['error'])? $data['error'] : null,
		);
		$this->updateTableRow(self::INVITATIONS_TABLE_ID, 'pid', $data);
	}

}
