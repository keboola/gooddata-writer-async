<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2014-05-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\Writer\SharedStorage;

class OptimizeSliHash extends AbstractJob
{

    public function prepare($params)
    {
        $this->checkParams($params, array('writerId'));
        $this->checkWriterExistence($params['writerId']);
        $this->sharedStorage->setWriterStatus($this->configuration->projectId, $params['writerId'], SharedStorage::WRITER_STATUS_MAINTENANCE);
        return [];
    }

    /**
     * @TODO works only with main project
     * required: email, role
     * optional: pid
     */
    public function run($job, $params, RestApi $restApi)
    {
        try {
            // Ensure that all other jobs are finished
            $this->sharedStorage->saveJob($job['id'], array('status' => SharedStorage::JOB_STATUS_WAITING));
            $i = 0;
            do {
                sleep($i * 60);
                $wait = false;
                foreach ($this->sharedStorage->fetchJobs($this->configuration->projectId, $this->configuration->writerId, 2) as $job) {
                    $queueIdArray = explode('.', $job['queueId']);
                    if ($job['status'] == SharedStorage::JOB_STATUS_PROCESSING && (isset($queueIdArray[2]) && $queueIdArray[2] != SharedStorage::SERVICE_QUEUE)) {
                        $wait = true;
                    }
                }
                $i++;
            } while ($wait);
            $this->sharedStorage->saveJob($job['id'], array('status' => SharedStorage::JOB_STATUS_PROCESSING));

            $bucketAttributes = $this->configuration->bucketAttributes();
            $restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
            $restApi->optimizeSliHash($bucketAttributes['gd']['pid']);

            $this->sharedStorage->setWriterStatus($job['projectId'], $job['writerId'], SharedStorage::WRITER_STATUS_READY);

            return [];
        } catch (\Exception $e) {
            $this->sharedStorage->setWriterStatus($job['projectId'], $job['writerId'], SharedStorage::WRITER_STATUS_READY);
            throw $e;
        }
    }
}
