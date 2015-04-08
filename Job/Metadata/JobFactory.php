<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\GoodDataWriter\Job\Metadata;

use Keboola\GoodDataWriter\Elasticsearch\Search;
use Keboola\GoodDataWriter\Writer\Configuration;
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
    /** @var Search */
    protected $jobSearch;

    public function __construct(
        $appName,
        QueueFactory $queueFactory,
        JobMapper $jobMapper,
        SyrupJobFactory $jobFactory,
        Search $jobSearch
    ) {
        $this->queue = $queueFactory->get($appName);
        $this->jobMapper = $jobMapper;
        $this->syrupJobFactory = $jobFactory;
        $this->jobSearch = $jobSearch;
    }

    public function setStorageApiClient(Client $client)
    {
        $this->syrupJobFactory->setStorageApiClient($client);
        return $this;
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

    public function getJobSearch()
    {
        return $this->jobSearch;
    }

    public function cancelWaitingJobs()
    {
        $jobs = $this->getJobSearch()->getJobs([
            'projectId' => $this->configuration->projectId,
            'status' => Job::STATUS_WAITING,
            'query' => 'params.writerId:"'.$this->configuration->writerId.'"'
        ]);
        foreach ($jobs as $jobData) {
            $index = $jobData['_index'];
            $type = $jobData['_type'];
            unset($jobData['_index']);
            unset($jobData['_type']);
            $job = new Job($jobData, $index, $type);
            $job->setStatus(Job::STATUS_CANCELLED);
            $this->update($job);
        }
    }
}
