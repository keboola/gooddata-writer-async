<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2014-05-12
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\Writer\Job;
use Keboola\GoodDataWriter\Writer\SharedStorage;

class OptimizeSliHash extends AbstractTask
{

    public function prepare($params)
    {
        $this->checkParams($params, ['writerId']);
        $this->checkWriterExistence($params['writerId']);
        $this->sharedStorage->setWriterStatus($this->configuration->projectId, $params['writerId'], SharedStorage::WRITER_STATUS_MAINTENANCE);
        return [];
    }

    /**
     * @TODO works only with main project
     * required: email, role
     * optional: pid
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);
        try {
            // Ensure that all other jobs are finished
            $job->setStatus(Job::STATUS_WAITING);
            $this->jobFactory->update($job);
            $i = 0;
            do {
                sleep($i * 60);
                $wait = false;
                foreach ($this->jobStorage->fetchJobs($this->configuration->projectId, $this->configuration->writerId, 2) as $job) {
                    $queueIdArray = explode('.', $job['queueId']);
                    if ($job['status'] == Job::STATUS_PROCESSING && (isset($queueIdArray[2]) && $queueIdArray[2] != Job::SERVICE_QUEUE)) {
                        $wait = true;
                    }
                }
                $i++;
            } while ($wait);
            $job->setStatus(Job::STATUS_PROCESSING);
            $this->jobFactory->update($job);

            $bucketAttributes = $this->configuration->bucketAttributes();
            $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
            $this->restApi->optimizeSliHash($bucketAttributes['gd']['pid']);

            $this->sharedStorage->setWriterStatus($this->configuration->projectId, $this->configuration->writerId, SharedStorage::WRITER_STATUS_READY);

            return [];
        } catch (\Exception $e) {
            $this->sharedStorage->setWriterStatus($this->configuration->projectId, $this->configuration->writerId, SharedStorage::WRITER_STATUS_READY);
            throw $e;
        }
    }
}
