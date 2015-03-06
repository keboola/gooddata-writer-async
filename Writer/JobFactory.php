<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\GoodDataWriter\Writer;

use Keboola\StorageApi\Client;
use Keboola\Syrup\Elasticsearch\JobMapper;
use Keboola\Syrup\Service\Queue\QueueFactory;
use Keboola\Syrup\Service\Queue\QueueService;
use Keboola\Syrup\Job\Metadata\JobFactory as SyrupJobFactory;

class JobFactory
{
    /**
     * @var Configuration
     */
    private $configuration;
    /**
     * @var QueueService
     */
    private $queue;
    /**
     * @var JobMapper
     */
    private $jobMapper;
    /**
     * @var SyrupJobFactory
     */
    private $syrupJobFactory;

    public function __construct(QueueFactory $queueFactory, JobMapper $jobMapper, SyrupJobFactory $jobFactory)
    {
        $this->queue = $queueFactory->get();
        $this->jobMapper = $jobMapper;
        $this->syrupJobFactory = $jobFactory;
    }

    public function setStorageApiClient(Client $client)
    {
        $this->syrupJobFactory->setStorageApiClient($client);
    }

    public function setConfiguration(Configuration $configuration)
    {
        $this->configuration = $configuration;
        return $this;
    }

    public function create($queue = null)
    {
        $syrupJob = $this->syrupJobFactory->create('run');
        $job = new Job($syrupJob->getData());
        $job->setWriterId($this->configuration->writerId);
        $job->setLockName(sprintf(
            '%s.%s.%s',
            $this->configuration->projectId,
            $this->configuration->writerId,
            $queue ?: Job::PRIMARY_QUEUE
        ));
        return $job;
    }

    public function save(Job $job)
    {
        return $this->jobMapper->create($job);
    }

    public function update(Job $job)
    {
        return $this->jobMapper->update($job);
    }

    public function enqueue($jobId, $delay = 0)
    {
        return $this->queue->enqueue($jobId, [], $delay);
    }
}
