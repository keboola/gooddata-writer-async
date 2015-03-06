<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-02
 */

namespace Keboola\GoodDataWriter\Writer;

use Keboola\GoodDataWriter\Task\Factory;
use Keboola\Syrup\Elasticsearch\JobMapper;
use Keboola\Syrup\Exception\MaintenanceException;
use Keboola\Temp\Temp;

use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\Service\EventLogger;
use Keboola\GoodDataWriter\Service\S3Client;

class JobExecutor extends \Keboola\Syrup\Job\Executor
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
        $job = new Job($job->getData());
        $jobParams = $job->getParams();
        $this->temp->initRunFolder();

        if (!isset($jobParams['writerId'])) {
            throw new \Exception(sprintf('Job %s is missing writerId', $job->getId()));
        }

        $eventLogger = new EventLogger($this->storageApi, $this->s3Client);

        $configuration = new Configuration($this->storageApi, $this->sharedStorage);
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
            throw new WrongParametersException(sprintf('Job %s has no tasks', $job->getId()));
        }

        $results = [];
        $logs = [];
        foreach ($jobParams['tasks'] as $i => $task) {
            if (!isset($task['name'])) {
                throw new \Exception(sprintf('Job %s has task %d without name', $job->getId(), $i));
            }
            if (!isset($task['params'])) {
                throw new \Exception(sprintf('Job %s has task %d without params', $job->getId(), $i));
            }

            $taskClass = $this->taskFactory->create($task['name']);
            $results[$i] = $taskClass->run($job, $i, $task['params'], isset($task['definition']) ? $task['definition'] : null);
            $logs[$i] = $taskClass->getLogs();
        }
        $job->setResult($results);
        $job->setAttribute('logs', $logs);

        $this->jobMapper->update($job);
    }
}
