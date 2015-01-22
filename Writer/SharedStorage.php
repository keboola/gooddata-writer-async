<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Writer;

use Doctrine\DBAL\Connection;
use Keboola\GoodDataWriter\GoodData\User;
use Keboola\GoodDataWriter\Service\Lock;
use Syrup\ComponentBundle\Encryption\Encryptor;


class SharedStorageException extends \Exception
{

}


class SharedStorage
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

	private $encryptor;
	private $db;


	public function __construct(Connection $db, Encryptor $encryptor)
	{
		$this->db = $db;
		$this->encryptor = $encryptor;
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

	public static function formatWriterData($data)
	{
		$result = array(
			'status' => $data['status'],
			'created' => array(
				'time' => $data['created_time'],
				'tokenId' => (int)$data['token_id'],
				'tokenDescription' => $data['token_desc']
			),
			'bucket' => $data['bucket'],
			'feats' => array(
				'date_facts' => (bool)$data['date_facts']
			)
		);

		if ($data['status'] == self::WRITER_STATUS_PREPARING) {
			$result['info'] = 'GoodData project is being prepared. You cannot perform any GoodData operations yet.';
		} elseif ($data['status'] == self::WRITER_STATUS_MAINTENANCE) {
			$result['info'] = 'Writer is undergoing maintenance. Jobs execution will be postponed.';
		} elseif ($data['status'] == self::WRITER_STATUS_DELETED) {
			$result['info'] = 'Writer is scheduled for removal. You cannot perform any operations any more.';
		} elseif (!empty($data['info'])) {
			$result['info'] = $data['info'];
		}

		return $result;
	}

	public function getWriter($projectId, $writerId)
	{
		$result = $this->db->fetchAssoc('SELECT * FROM writers WHERE project_id=? AND writer_id=?', array($projectId, $writerId));
		if (!$result) throw new SharedStorageException('Writer ' . $writerId . ' does not exist in Shared Config');

		return self::formatWriterData($result);
	}

	public function getActiveWriters($projectId)
	{
		$result = array();
		foreach ($this->db->fetchAll('SELECT * FROM writers WHERE project_id=? AND deleted_time IS NULL', array($projectId)) as $writer) {
			$result[] = self::formatWriterData($writer);
		}
		return $result;
	}

	public function writerExists($projectId, $writerId)
	{
		return (bool)$this->db->fetchColumn('SELECT COUNT(*) FROM writers WHERE project_id=? AND writer_id=?', array($projectId, $writerId));
	}

	public function deleteWriter($projectId, $writerId)
	{
		$this->db->update(
			'writers',
			array('status' => self::WRITER_STATUS_DELETED, 'deleted_time' => date('c')),
			array('project_id' => $projectId, 'writer_id' => $writerId)
		);
	}


	public static function isJobFinished($status)
	{
		return !in_array($status, array(SharedStorage::JOB_STATUS_WAITING, SharedStorage::JOB_STATUS_PROCESSING));
	}

	public function getDomainUser($domain)
	{
		$result = $this->db->fetchAssoc('SELECT * FROM domains WHERE name=?', array($domain));
		if (!$result || !isset($result['name']))
			throw new SharedStorageException(sprintf("User for domain '%s' does not exist", $domain));

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
		$query = $this->db->createQueryBuilder()
			->select('*')
			->from('jobs')
			->where('projectId = ?')
			->andWhere('writerId = ?')
			->setParameters(array(
				$projectId,
				$writerId
			));
		if ($days) {
			$query->andWhere('createdTime >= DATE_SUB(NOW(), INTERVAL ? DAY)')
			->setParameter(2, $days);
		}

		$result = array();
		foreach ($query->execute()->fetchAll() as $job) {
			$result[] = $this->decodeJob($job);
		}
		return $result;
	}

	/**
	 * @param $jobId
	 * @param null $writerId
	 * @param null $projectId
	 * @return mixed
	 */
	public function fetchJob($jobId, $projectId=null, $writerId=null)
	{
		$query = $this->db->createQueryBuilder()
			->select('*')
			->from('jobs')
			->where('id = ?')
			->setParameter(0, $jobId);
		if ($writerId && $projectId) {
			$query->andWhere('projectId = ?')
				->andWhere('writerId = ?')
				->setParameter(1, $projectId)
				->setParameter(2, $writerId);
		}
		$result = $query->execute()->fetchAll();

		return count($result)? $this->decodeJob(current($result)) : false;
	}

	/**
	 *
	 */
	public function fetchBatch($batchId)
	{
		$query = $this->db->createQueryBuilder()
			->select('*')
			->from('jobs')
			->where('batchId = ?')
			->setParameter(0, $batchId);

		$result = array();
		foreach ($query->execute()->fetchAll() as $job) {
			$result[] = $this->decodeJob($job);
		}
		return $result;
	}

	private function decodeJob($job)
	{
		$keysToDecode = array('parameters', 'result', 'logs', 'debug');
		foreach ($keysToDecode as $key) {
			if (isset($job[$key])) {
				$decodedParameters = json_decode($job[$key], true);
				if (is_array($decodedParameters)) {
					$job[$key] = $decodedParameters;
				}
			}
		}
		return $job;
	}

	/**
	 *
	 */
	public function cancelJobs($projectId, $writerId)
	{
		$this->db->update('jobs', array('status' => self::JOB_STATUS_CANCELLED), array(
			'projectId' => $projectId,
			'writerId' => $writerId,
			'status' => self::JOB_STATUS_WAITING
		));
	}

	/**
	 * Create new job
	 */
	public function createJob($jobId, $projectId, $writerId, $data, $queue=self::PRIMARY_QUEUE)
	{
		$jobData = array(
			'id' => $jobId,
			'runId' => isset($data['runId'])? $data['runId'] : $jobId,
			'batchId' => isset($data['batchId'])? $data['batchId'] : $jobId,
			'projectId' => $projectId,
			'writerId' => $writerId,
			'token' => null,
			'tokenId' => null,
			'tokenDesc' => null,
			'createdTime' => date('c'),
			'startTime' => null,
			'endTime' => null,
			'command' => null,
			'dataset' => null,
			'parameters' => array(),
			'result' => array(),
			'status' => self::JOB_STATUS_WAITING,
			'logs' => array(),
			'debug' => null,
			'queueId' => sprintf('%s.%s.%s', $projectId, $writerId, $queue)
		);
		$jobData = array_merge($jobData, $data);

		$this->saveJob($jobId, $jobData, true);
		return $jobData;
	}

	/**
	 * Update existing job
	 */
	public function saveJob($jobId, $fields, $create=false)
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

		if ($create) {
			$this->db->insert('jobs', $fields);
		} else {
			unset($fields['id']);
			$this->db->update('jobs', $fields, array('id' => $jobId));
		}
	}

	/**
	 *
	 */
	public static function jobToApiResponse(array $job)
	{
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
		foreach ($logs as &$log) {
			if (is_array($log)) foreach ($log as &$v) {
				$url = parse_url($v);
				if (empty($url['host'])) {
					$v = 'https://connection.keboola.com/admin/utils/logs?file=' . $v;
				}
			} else {
				$url = parse_url($log);
				if (empty($url['host'])) {
					$log = 'https://connection.keboola.com/admin/utils/logs?file=' . $log;
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
			'createdTime' => date('c', strtotime($job['createdTime'])),
			'startTime' => !empty($job['startTime']) ? date('c', strtotime($job['startTime'])) : null,
			'endTime' => !empty($job['endTime']) ? date('c', strtotime($job['endTime'])) : null,
			'command' => $job['command'],
			'dataset' => $job['dataset'],
			'parameters' => $job['parameters'],
			'result' => $job['result'],
			'gdWriteStartTime' => false,
			'status' => $job['status'],
			'logs' => $logs
		);

		return $result;
	}

	/**
	 *
	 */
	public static function batchToApiResponse($batchId, array $jobs)
	{
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
			$job = self::jobToApiResponse($job);

			if (!$data['projectId']) {
				$data['projectId'] = $job['projectId'];
			} elseif ($data['projectId'] != $job['projectId']) {
				throw new SharedStorageException(sprintf('ProjectId of job %s: %s does not match projectId %s of previous job.',
					$job['id'], $job['projectId'], $data['projectId']));
			}
			if (!$data['writerId']) {
				$data['writerId'] = $job['writerId'];
			} elseif ($data['writerId'] != $job['writerId']) {
				throw new SharedStorageException(sprintf('WriterId of job %s: %s does not match writerId %s of previous job.',
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
		if ($data['status'] == self::JOB_STATUS_WAITING && $data['startTime']) {
			$data['startTime'] = null;
		}

		return $data;
	}


	/**
	 * Save project to shared config
	 */
	public function saveProject($projectId, $writerId, $pid, $accessToken=null, $keepOnRemoval=false)
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
	public function getProjects($projectId=null, $writerId=null)
	{
		if ($projectId && $writerId) {
			return $this->db->fetchAll('SELECT * FROM projects WHERE project_id=? AND writer_id=? AND removal_time IS NULL',
				array($projectId, $writerId));
		} else {
			return $this->db->fetchAll('SELECT * FROM projects WHERE removal_time IS NULL');
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
