<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-02
 */

namespace Keboola\GoodDataWriter\Writer;

use Doctrine\DBAL\Connection;
use Keboola\GoodDataWriter\Service\S3Client;
use Keboola\GoodDataWriter\Exception\SharedStorageException;

class JobStorage
{

    const JOBS_TABLE_ID = 'in.c-wr-gooddata.jobs';

    const JOB_STATUS_WAITING = 'waiting';
    const JOB_STATUS_PROCESSING = 'processing';
    const JOB_STATUS_SUCCESS = 'success';
    const JOB_STATUS_ERROR = 'error';
    const JOB_STATUS_CANCELLED = 'cancelled';

    const PRIMARY_QUEUE = 'primary';
    const SECONDARY_QUEUE = 'secondary';
    const SERVICE_QUEUE = 'service';

    /**
     * @var Connection
     */
    private $db;


    public function __construct(Connection $db)
    {
        $this->db = $db;
    }

    public static function isJobFinished($status)
    {
        return !in_array($status, [self::JOB_STATUS_WAITING, self::JOB_STATUS_PROCESSING]);
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
            $result[] = self::decodeJob($job);
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

        return count($result)? self::decodeJob(current($result)) : false;
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
            $result[] = self::decodeJob($job);
        }
        return $result;
    }

    private static function decodeJob($job)
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
}
