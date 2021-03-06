<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Writer;

use Doctrine\DBAL\Connection;
use Keboola\GoodDataWriter\Exception\SharedStorageException;
use Keboola\GoodDataWriter\GoodData\User;
use Keboola\GoodDataWriter\Service\Lock;
use Keboola\GoodDataWriter\Service\S3Client;
use Keboola\Syrup\Encryption\Encryptor;

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
        $this->db->insert('writers', [
            'project_id' => $projectId,
            'writer_id' => $writerId,
            'bucket' => $bucket,
            'status' => self::WRITER_STATUS_PREPARING,
            'token_id' => $tokenId,
            'token_desc' => $tokenDescription,
            'created_time' => date('c')
        ]);
    }

    public function updateWriter($projectId, $writerId, $values)
    {
        $this->db->update('writers', $values, ['project_id' => $projectId, 'writer_id' => $writerId]);
    }

    public function setWriterStatus($projectId, $writerId, $status)
    {
        $this->updateWriter($projectId, $writerId, [
            'status' => $status
        ]);
    }

    public static function formatWriterData($data)
    {
        $result = [
            'status' => $data['status'],
            'created' => [
                'time' => $data['created_time'],
                'tokenId' => (int)$data['token_id'],
                'tokenDescription' => $data['token_desc']
            ],
            'bucket' => $data['bucket'],
            'feats' => [
                'date_facts' => (bool)$data['date_facts']
            ]
        ];

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
        $result = $this->db->fetchAssoc('SELECT * FROM writers WHERE project_id=? AND writer_id=?', [$projectId, $writerId]);
        if (!$result) {
            throw new SharedStorageException('Writer ' . $writerId . ' does not exist in Shared Config');
        }

        return self::formatWriterData($result);
    }

    public function getActiveWriters($projectId)
    {
        $result = [];
        foreach ($this->db->fetchAll('SELECT * FROM writers WHERE project_id=? AND deleted_time IS NULL', [$projectId]) as $writer) {
            $result[] = self::formatWriterData($writer);
        }
        return $result;
    }

    public function writerExists($projectId, $writerId)
    {
        return (bool)$this->db->fetchColumn('SELECT COUNT(*) FROM writers WHERE project_id=? AND writer_id=?', [$projectId, $writerId]);
    }

    public function deleteWriter($projectId, $writerId)
    {
        $this->db->update(
            'writers',
            ['status' => self::WRITER_STATUS_DELETED, 'deleted_time' => date('c')],
            ['project_id' => $projectId, 'writer_id' => $writerId]
        );
    }


    public static function isJobFinished($status)
    {
        return !in_array($status, [SharedStorage::JOB_STATUS_WAITING, SharedStorage::JOB_STATUS_PROCESSING]);
    }

    public function getDomainUser($domain)
    {
        $result = $this->db->fetchAssoc('SELECT * FROM domains WHERE name=?', [$domain]);
        if (!$result || !isset($result['name'])) {
            throw new SharedStorageException(sprintf("User for domain '%s' does not exist", $domain));
        }

        $user = new User();
        $user->domain = $result['name'];
        $user->username = $result['username'];
        $user->password = $this->encryptor->decrypt($result['password']);
        $user->uid = $result['uid'];

        return $user;
    }

    public function saveDomain($name, $username, $password, $uid)
    {
        $this->db->insert('domains', [
            'name' => $name,
            'username' => $username,
            'password' => $this->encryptor->encrypt($password),
            'uid' => $uid
        ]);
    }


    public function fetchJobs($projectId, $writerId, $days = 7)
    {
        $query = $this->db->createQueryBuilder()
            ->select('*')
            ->from('jobs')
            ->where('projectId = ?')
            ->andWhere('writerId = ?')
            ->setParameters([
                $projectId,
                $writerId
            ]);
        if ($days) {
            $query->andWhere('createdTime >= DATE_SUB(NOW(), INTERVAL ? DAY)')
            ->setParameter(2, $days);
        }

        $result = [];
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
    public function fetchJob($jobId, $projectId = null, $writerId = null)
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

        $result = [];
        foreach ($query->execute()->fetchAll() as $job) {
            $result[] = $this->decodeJob($job);
        }
        return $result;
    }

    private function decodeJob($job)
    {
        $keysToDecode = ['parameters', 'result', 'logs', 'debug'];
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
        $this->db->update('jobs', ['status' => self::JOB_STATUS_CANCELLED], [
            'projectId' => $projectId,
            'writerId' => $writerId,
            'status' => self::JOB_STATUS_WAITING
        ]);
    }

    /**
     * Create new job
     */
    public function createJob($jobId, $projectId, $writerId, $data, $queue = self::PRIMARY_QUEUE)
    {
        $jobData = [
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
            'parameters' => [],
            'result' => [],
            'status' => self::JOB_STATUS_WAITING,
            'logs' => [],
            'debug' => null,
            'queueId' => sprintf('%s.%s.%s', $projectId, $writerId, $queue)
        ];
        $jobData = array_merge($jobData, $data);

        $this->saveJob($jobId, $jobData, true);
        return $jobData;
    }

    /**
     * Update existing job
     */
    public function saveJob($jobId, $fields, $create = false)
    {
        $keysToEncode = ['parameters', 'result', 'logs', 'debug'];
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
            $this->db->update('jobs', $fields, ['id' => $jobId]);
        }
    }

    /**
     *
     */
    public static function jobToApiResponse(array $job, S3Client $s3Client = null)
    {
        if (isset($job['parameters']['accessToken'])) {
            $job['parameters']['accessToken'] = '***';
        }
        if (isset($job['parameters']['password'])) {
            $job['parameters']['password'] = '***';
        }

        $logs = is_array($job['logs']) ? $job['logs'] : [];
        if (!empty($job['definition'])) {
            $logs['DataSet Definition'] = $job['definition'];
        }

        // Find private links and make them accessible
        if ($s3Client) {
            foreach ($logs as &$log) {
                if (is_array($log)) {
                    foreach ($log as &$v) {
                        $url = parse_url($v);
                        if (empty($url['host'])) {
                            $v = $s3Client->getPublicLink($v);
                        }
                    }
                } else {
                    $url = parse_url($log);
                    if (empty($url['host'])) {
                        $log = $s3Client->getPublicLink($log);
                    }
                }
            }
        }

        $result = [
            'id' => (int) $job['id'],
            'batchId' => (int) $job['batchId'],
            'runId' => (int) $job['runId'],
            'projectId' => (int) $job['projectId'],
            'writerId' => (string) $job['writerId'],
            'queueId' => !empty($job['queueId']) ? $job['queueId'] : sprintf('%s.%s.%s', $job['projectId'], $job['writerId'], self::PRIMARY_QUEUE),
            'token' => [
                'id' => (int) $job['tokenId'],
                'description' => $job['tokenDesc'],
            ],
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
        ];

        return $result;
    }

    /**
     *
     */
    public static function batchToApiResponse($batchId, array $jobs, S3Client $s3Client = null)
    {
        $data = [
            'batchId' => (int)$batchId,
            'projectId' => null,
            'writerId' => null,
            'queueId' => null,
            'createdTime' => date('c'),
            'startTime' => date('c'),
            'endTime' => null,
            'status' => null,
            'jobs' => []
        ];
        $cancelledJobs = 0;
        $waitingJobs = 0;
        $processingJobs = 0;
        $errorJobs = 0;
        $successJobs = 0;
        foreach ($jobs as $job) {
            $job = self::jobToApiResponse($job, $s3Client);

            if (!$data['projectId']) {
                $data['projectId'] = $job['projectId'];
            } elseif ($data['projectId'] != $job['projectId']) {
                throw new SharedStorageException(sprintf(
                    'ProjectId of job %s: %s does not match projectId %s of previous job.',
                    $job['id'],
                    $job['projectId'],
                    $data['projectId']
                ));
            }
            if (!$data['writerId']) {
                $data['writerId'] = $job['writerId'];
            } elseif ($data['writerId'] != $job['writerId']) {
                throw new SharedStorageException(sprintf(
                    'WriterId of job %s: %s does not match writerId %s of previous job.',
                    $job['id'],
                    $job['projectId'],
                    $data['projectId']
                ));
            }

            if ($job['queueId'] && $job['queueId'] != self::PRIMARY_QUEUE) {
                $data['queueId'] = $job['queueId'];
            }

            if ($job['createdTime'] < $data['createdTime']) {
                $data['createdTime'] = $job['createdTime'];
            }
            if ($job['startTime'] < $data['startTime']) {
                $data['startTime'] = $job['startTime'];
            }
            if ($job['endTime'] > $data['endTime']) {
                $data['endTime'] = $job['endTime'];
            }
            $data['jobs'][] = $job;
            if ($job['status'] == self::JOB_STATUS_WAITING) {
                $waitingJobs++;
            } elseif ($job['status'] == self::JOB_STATUS_PROCESSING) {
                $processingJobs++;
            } elseif ($job['status'] == self::JOB_STATUS_CANCELLED) {
                $cancelledJobs++;
            } elseif ($job['status'] == self::JOB_STATUS_ERROR) {
                $errorJobs++;
                $data['result'][$job['id']] = $job['result'];
            } else {
                $successJobs++;
            }
        }

        if (!$data['queueId']) {
            $data['queueId'] = sprintf('%s.%s.%s', $data['projectId'], $data['writerId'], self::PRIMARY_QUEUE);
        }

        if ($cancelledJobs > 0) {
            $data['status'] = self::JOB_STATUS_CANCELLED;
        } elseif ($processingJobs > 0) {
            $data['status'] = self::JOB_STATUS_PROCESSING;
        } elseif ($waitingJobs > 0) {
            $data['status'] = self::JOB_STATUS_WAITING;
        } elseif ($errorJobs > 0) {
            $data['status'] = self::JOB_STATUS_ERROR;
        } else {
            $data['status'] = self::JOB_STATUS_SUCCESS;
        }
        if ($data['status'] == self::JOB_STATUS_WAITING && $data['startTime']) {
            $data['startTime'] = null;
        }

        return $data;
    }


    /**
     * Save project to shared config
     */
    public function saveProject($projectId, $writerId, $pid, $accessToken = null, $keepOnRemoval = false)
    {
        $this->db->executeUpdate('REPLACE INTO projects SET pid=?, project_id=?, writer_id=?, created_time=?, access_token=?, keep_on_removal=?', [
            $pid,
            $projectId,
            $writerId,
            date('c'),
            $accessToken,
            $keepOnRemoval
        ]);
    }

    /**
     * @param $uid
     * @param $email
     * @param $job
     */
    public function saveUser($projectId, $writerId, $uid, $email)
    {
        $this->db->executeUpdate(
            'REPLACE INTO users SET uid=?, email=?, project_id=?, writer_id=?, created_time=?',
            [$uid, strtolower($email), $projectId, $writerId, date('c')]
        );
    }


    /**
     * @param $projectId
     * @param $writerId
     * @return mixed
     */
    public function getProjects($projectId = null, $writerId = null)
    {
        if ($projectId && $writerId) {
            return $this->db->fetchAll(
                'SELECT * FROM projects WHERE project_id=? AND writer_id=? AND removal_time IS NULL',
                [$projectId, $writerId]
            );
        } else {
            return $this->db->fetchAll('SELECT * FROM projects WHERE removal_time IS NULL');
        }
    }

    public function projectBelongsToWriter($projectId, $writerId, $pid)
    {
        $result = $this->db->fetchAssoc(
            'SELECT * FROM projects WHERE pid=? AND project_id=? AND writer_id=?',
            [$pid, $projectId, $writerId]
        );
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
        $this->db->executeQuery('UPDATE projects SET deleted_time=NOW() WHERE pid IN (?)', [$pids], [Connection::PARAM_STR_ARRAY]);
        $this->db->executeQuery('UPDATE projects SET removal_time=NOW() WHERE pid IN (?) AND removal_time IS NULL', [$pids], [Connection::PARAM_STR_ARRAY]);
    }


    public function enqueueProjectToDelete($projectId, $writerId, $pid)
    {
        $this->db->update('projects', ['removal_time' => date('c')], ['pid' => $pid, 'project_id' => $projectId, 'writer_id' => $writerId]);
    }


    /**
     * @param $projectId
     * @param $writerId
     * @return mixed
     */
    public function getUsers($projectId, $writerId)
    {
        return $this->db->fetchAll('SELECT * FROM users WHERE project_id=? AND writer_id=?', [$projectId, $writerId]);
    }

    public function userBelongsToWriter($projectId, $writerId, $email)
    {
        $result = $this->db->fetchAssoc('SELECT * FROM users WHERE email=?', [$email]);
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
        $this->db->executeQuery('UPDATE users SET deleted_time=NOW() WHERE uid IN (?)', [$ids], [Connection::PARAM_STR_ARRAY]);
        $this->db->executeQuery('UPDATE users SET removal_time=NOW() WHERE uid IN (?) AND removal_time IS NULL', [$ids], [Connection::PARAM_STR_ARRAY]);
    }

    /**
     * @param $projectId
     * @param $writerId
     * @param $uid
     * @param $email
     */
    public function enqueueUserToDelete($projectId, $writerId, $uid)
    {
        $this->db->update('users', ['removal_time' => date('c')], ['uid' => $uid, 'project_id' => $projectId, 'writer_id' => $writerId]);
    }


    public function logInvitation($data)
    {
        $this->db->executeUpdate(
            'REPLACE INTO project_invitations SET pid=?, sender=?, created_time=?, accepted_time=?, status=?, error=?',
            [$data['pid'], $data['sender'], $data['createDate'], date('c'), 'ok', isset($data['error'])? $data['error'] : null]
        );
    }
}
