<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-02
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Job\Metadata\Job;
use Keboola\GoodDataWriter\StorageApi\CachedClient;
use Keboola\GoodDataWriter\Task\Factory;
use Keboola\GoodDataWriter\Writer\Configuration;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Keboola\StorageApi\Event;
use Keboola\Syrup\Elasticsearch\JobMapper;
use Keboola\Syrup\Exception\MaintenanceException;
use Keboola\Syrup\Exception\UserException;
use Keboola\Temp\Temp;

use Keboola\GoodDataWriter\StorageApi\EventLogger;
use Keboola\GoodDataWriter\Aws\S3Client;

class Executor extends \Keboola\Syrup\Job\Executor
{
    /**
     * @var Factory
     */
    protected $taskFactory;
    /**
     * @var SharedStorage
     */
    protected $sharedStorage;
    /**
     * @var S3Client
     */
    protected $s3Client;
    /**
     * @var Temp
     */
    protected $temp;
    /**
     * @var JobMapper
     */
    protected $jobMapper;


    /**
     *
     */
    public function __construct(
        Factory $taskFactory,
        SharedStorage $sharedStorage,
        S3Client $s3Client,
        Temp $temp,
        JobMapper $jobMapper
    ) {
        $this->taskFactory = $taskFactory;
        $this->sharedStorage = $sharedStorage;
        $this->s3Client = $s3Client;
        $this->temp = $temp;
        $this->jobMapper = $jobMapper;
    }

    public function execute(\Keboola\Syrup\Job\Metadata\Job $job)
    {
        $job = new Job($job->getData(), $job->getIndex(), $job->getType());
        $jobParams = $job->getParams();
        $this->temp->initRunFolder();

        if (!isset($jobParams['writerId'])) {
            throw new \Exception(sprintf('Job %s is missing writerId', $job->getId()));
        }

        $eventLogger = new EventLogger($this->storageApi, $this->s3Client);

        $configuration = new Configuration(new CachedClient($this->storageApi), $this->sharedStorage);
        $configuration->setWriterId($jobParams['writerId']);

        $this->taskFactory
            ->setStorageApiClient($this->storageApi)
            ->setEventLogger($eventLogger)
            ->setConfiguration($configuration)
            ->setJob($job);

        // Check writer maintenance
        $serviceRun = strpos($job->getLockName(), Job::SERVICE_QUEUE) !== false;
        $writerInfo = $this->sharedStorage->getWriter($job->getProject()['id'], $jobParams['writerId']);
        if (!$serviceRun && $writerInfo['status'] == SharedStorage::WRITER_STATUS_MAINTENANCE) {
            throw new MaintenanceException('Writer is undergoing maintenance');
        }

        if (!isset($jobParams['tasks']) || !count($jobParams['tasks'])) {
            throw new UserException(sprintf('Job %s has no tasks', $job->getId()));
        }

        $results = [];
        foreach ($jobParams['tasks'] as $i => $task) {
            if (!isset($task['name'])) {
                throw new \Exception(sprintf('Job %s has task %d without name', $job->getId(), $i));
            }
            if (!isset($task['params'])) {
                throw new \Exception(sprintf('Job %s has task %d without params', $job->getId(), $i));
            }

            try {
                $taskClass = $this->taskFactory->create($task['name']);
                $results[$i] = $taskClass->run(
                    $job,
                    $i,
                    $task['params'],
                    isset($task['definition']) ? $task['definition'] : null
                );
            } catch (\Exception $e) {
                if ($e instanceof UserException) {
                    $message = $e->getMessage();
                    if (substr($e->getMessage(), 0, 12) == 'User error: ') {
                        $message = substr($e->getMessage(), 12);
                    }
                    $eventLogger->log($job->getId(), $job->getRunId(), $message, $e->getData(), null, Event::TYPE_ERROR);
                    throw new UserException(
                        sprintf('Task %d (%s): %s', $i, ucfirst($task['name']), $message),
                        $e,
                        $e->getData()
                    );
                } else {
                    throw $e;
                }
            }

            $eventLogger->log($job->getId(), $job->getRunId(), sprintf('Task %d (%s) finished', $i, $task['name']), $task['params']);
        }

        return $results;
    }
}
