<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Writer;

use Doctrine\DBAL\Connection;
use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\GoodData\User;
use Keboola\GoodDataWriter\Service\Lock;
use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\GoodDataWriter\Service\S3Client,
	Keboola\GoodDataWriter\Service\StorageApiConfiguration;
use Syrup\ComponentBundle\Encryption\Encryptor;


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

	const WRITER_STATUS_PREPARING = 'preparing';
	const WRITER_STATUS_READY = 'ready';
	const WRITER_STATUS_ERROR = 'error';
	const WRITER_STATUS_MAINTENANCE = 'maintenance';
	const WRITER_STATUS_DELETED = 'deleted';

	const JOB_STATUS_WAITING = 'waiting';
	const JOB_STATUS_PROCESSING = 'processing';
	const JOB_STATUS_SUCCESS = 'success';
	const JOB_STATUS_ERROR = 'error';
	const JOB_STATUS_CANCELLED = 'cancelled';

	const PRIMARY_QUEUE = 'primary';
	const SECONDARY_QUEUE = 'secondary';
	const SERVICE_QUEUE = 'service';

	protected $storageApiClient;
	private $encryptor;
	private $db;


	public function __construct(AppConfiguration $appConfiguration, Encryptor $encryptor)
	{
		$this->storageApiClient = new StorageApiClient(array(
			'token' => $appConfiguration->sharedSapi_token,
			'url' => $appConfiguration->sharedSapi_url,
			'userAgent' => $appConfiguration->userAgent
		));
        $this->encryptor = $encryptor;

		$config = new \Doctrine\DBAL\Configuration();
		$connectionParams = array(
			'dbname' => $appConfiguration->db_name,
			'user' => $appConfiguration->db_user,
			'password' => $appConfiguration->db_password,
			'host' => $appConfiguration->db_host,
			'driver' => 'pdo_mysql',
			'charset' => 'utf8'
		);
		$this->db = \Doctrine\DBAL\DriverManager::getConnection($connectionParams, $config);
		$this->db->exec('SET wait_timeout = 31536000;');
	}

	public function getLock($name)
	{
		return new Lock($this->db, $name);
	}

	public function createWriter($projectId, $writerId, $bucket, $tokenId, $tokenDescription)
	{
		$this->db->insert('writers', array(
			'project_id' => $projectId,
			'writer_id' => $writerId,
			'bucket' => $bucket,
			'status' => self::WRITER_STATUS_PREPARING,
			'token_id' => $tokenId,
			'token_desc' => $tokenDescription,
			'created_time' => date('c')
		));
	}

	public function updateWriter($projectId, $writerId, $values)
	{
		$this->db->update('writers', $values, array('project_id' => $projectId, 'writer_id' => $writerId));
	}

	public function setWriterStatus($projectId, $writerId, $status)
	{
		$this->updateWriter($projectId, $writerId, array(
			'status' => $status
		));
	}

	public function getWriter($projectId, $writerId)
	{
		$result = $this->db->fetchAssoc('SELECT * FROM writers WHERE project_id=? AND writer_id=?', array($projectId, $writerId));
		if (!$result) throw new SharedConfigException('Writer ' . $writerId . ' does not exist in Shared Config');

		$return = array(
			'status' => $result['status'],
			'created' => array(
				'time' => $result['created_time'],
				'tokenId' => (int)$result['token_id'],
				'tokenDescription' => $result['token_desc']
			),
			'bucket' => $result['bucket'],
			'feats' => array(
				'date_facts' => (bool)$result['date_facts'],
				'cl_tool' => (bool)$result['cl_tool']
			)
		);

		if ($result['status'] == self::WRITER_STATUS_PREPARING) {
			$return['info'] = 'GoodData project is being prepared. You cannot perform any GoodData operations yet.';
		} elseif ($result['status'] == self::WRITER_STATUS_MAINTENANCE) {
			$return['info'] = 'Writer is undergoing maintenance. Jobs execution will be postponed.';
		} elseif ($result['status'] == self::WRITER_STATUS_DELETED) {
			$return['info'] = 'Writer is scheduled for removal. You cannot perform any operations any more.';
		} elseif (!empty($result['info'])) {
			$return['info'] = $result['info'];
		}

		return $return;
	}

	public function writerExists($projectId, $writerId)
	{
		return (bool)$this->db->fetchColumn('SELECT COUNT(*) FROM writers WHERE project_id=? AND writer_id=?', array($projectId, $writerId));
	}

	public function deleteWriter($projectId, $writerId)
	{
		$this->db->update('writers', array('status' => self::WRITER_STATUS_DELETED, 'deleted_time' => date('c')), array('project_id' => $projectId, 'writer_id' => $writerId));
	}


	public static function isJobFinished($status)
	{
		return !in_array($status, array(SharedConfig::JOB_STATUS_WAITING, SharedConfig::JOB_STATUS_PROCESSING));
	}

	public function getDomainUser($domain)
	{
		$result = $this->db->fetchAssoc('SELECT * FROM domains WHERE name=?', array($domain));
		if (!$result || !isset($result['name']))
			throw new SharedConfigException(sprintf("User for domain '%s' does not exist", $domain));

		$user = new User();
		$user->domain = $result['name'];
		$user->username = $result['username'];
		$user->password = $this->encryptor->decrypt($result['password']);
		$user->uid = $result['uid'];

		return $user;
	}

	public function saveDomain($name, $username, $password, $uid)
	{
		$this->db->insert('domains', array(
			'name' => $name,
			'username' => $username,
			'password' => $this->encryptor->encrypt($password),
			'uid' => $uid
		));
	}


	public function fetchJobs($projectId, $writerId, $days=7)
	{
		return $this->fetchTableRows(self::JOBS_TABLE_ID, 'projectIdWriterId', $projectId . '.' . $writerId, array(
			'changedSince' => '-' . $days . ' days',
			'limit' => 1000
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
			$data['jobs'][] = $job;
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
	public function saveProject($projectId, $writerId, $pid, $accessToken=null, $keepOnRemoval = false)
	{
		$this->db->executeUpdate('REPLACE INTO projects SET pid=?, project_id=?, writer_id=?, created_time=?, '
			. 'access_token=?, keep_on_removal=?', array($pid, $projectId, $writerId, date('c'), $accessToken, $keepOnRemoval));
	}

	/**
	 * @param $uid
	 * @param $email
	 * @param $job
	 */
	public function saveUser($projectId, $writerId, $uid, $email)
	{
		$this->db->executeUpdate('REPLACE INTO users SET uid=?, email=?, project_id=?, writer_id=?, created_time=?',
			array($uid, strtolower($email), $projectId, $writerId, date('c')));
	}


	/**
	 * @param $projectId
	 * @param $writerId
	 * @return mixed
	 */
	public function getProjects($projectId = null, $writerId = null)
	{
		if ($projectId && $writerId) {
			return $this->db->fetchAll('SELECT * FROM projects WHERE project_id=? AND writer_id=? AND removal_time=NULL',
				array($projectId, $writerId));
		} else {
			return $this->db->fetchAll('SELECT * FROM projects WHERE removal_time=NULL');
		}
	}

	public function projectBelongsToWriter($projectId, $writerId, $pid)
	{
		$result = $this->db->fetchAssoc('SELECT * FROM projects WHERE pid=? AND project_id=? AND writer_id=?',
			array($pid, $projectId, $writerId));
		return (bool)$result;
	}

	/**
	 * @return array
	 */
	public function projectsToDelete()
	{
		return $this->db->fetchAll('SELECT * FROM projects WHERE deleted_time IS NULL AND DATEDIFF(CURRENT_DATE, DATE(removal_time)) > 30');
	}

	/**
	 * @param $pids
	 */
	public function markProjectsDeleted($pids)
	{
		$this->db->executeQuery('UPDATE projects SET deleted_time=NOW() WHERE pid IN (?)', array($pids), array(Connection::PARAM_STR_ARRAY));
		$this->db->executeQuery('UPDATE projects SET removal_time=NOW() WHERE pid IN (?) AND removal_time IS NULL', array($pids), array(Connection::PARAM_STR_ARRAY));
	}


	public function enqueueProjectToDelete($projectId, $writerId, $pid)
	{
		$this->db->update('projects', array('removal_time' => date('c')), array('pid' => $pid, 'project_id' => $projectId, 'writer_id' => $writerId));
	}


	/**
	 * @param $projectId
	 * @param $writerId
	 * @return mixed
	 */
	public function getUsers($projectId, $writerId)
	{
		return $this->db->fetchAll('SELECT * FROM users WHERE project_id=? AND writer_id=?', array($projectId, $writerId));
	}

	public function userBelongsToWriter($projectId, $writerId, $email)
	{
		$result = $this->db->fetchAssoc('SELECT * FROM users WHERE email=?', array($email));
		return $result && ($result['project_id'] == $projectId) && ($result['writer_id'] == $writerId);
	}

	/**
	 * @return array
	 */
	public function usersToDelete()
	{
		return $this->db->fetchAll('SELECT * FROM users WHERE deleted_time IS NULL AND DATEDIFF(CURRENT_DATE, DATE(removal_time)) > 30');
	}

	/**
	 * @param $ids
	 */
	public function markUsersDeleted($ids)
	{
		$this->db->executeQuery('UPDATE users SET deleted_time=NOW() WHERE uid IN (?)', array($ids), array(Connection::PARAM_STR_ARRAY));
		$this->db->executeQuery('UPDATE users SET removal_time=NOW() WHERE uid IN (?) AND removal_time IS NULL', array($ids), array(Connection::PARAM_STR_ARRAY));
	}

	/**
	 * @param $projectId
	 * @param $writerId
	 * @param $uid
	 * @param $email
	 */
	public function enqueueUserToDelete($projectId, $writerId, $uid)
	{
		$this->db->update('users', array('removal_time' => date('c')), array('uid' => $uid, 'project_id' => $projectId, 'writer_id' => $writerId));
	}


	public function logInvitation($data)
	{
		$this->db->executeUpdate('REPLACE INTO project_invitations SET pid=?, sender=?, created_time=?, accepted_time=?, status=?, error=?',
			array($data['pid'], $data['sender'], $data['createDate'], date('c'), $data['status'], isset($data['error'])? $data['error'] : null));
	}

}
