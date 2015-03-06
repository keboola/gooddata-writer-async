<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2014-05-14
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\Writer\Job;
use Keboola\GoodDataWriter\Writer\SharedStorage;

class WaitForInvitation extends AbstractTask
{

    public function prepare($params)
    {

    }

    /**
     * required:
     * optional:
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);
        $this->checkParams($params, ['try']);
        $bucketAttributes = $this->configuration->bucketAttributes();

        $this->restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);
        if ($this->restApi->hasAccessToProject($bucketAttributes['gd']['pid'])) {
            $this->restApi->addUserToProject($bucketAttributes['gd']['uid'], $bucketAttributes['gd']['pid']);
            $this->sharedStorage->setWriterStatus($this->configuration->projectId, $this->configuration->writerId, SharedStorage::WRITER_STATUS_READY);
            $this->configuration->updateBucketAttribute('waitingForInvitation', null);

        } else {
            if ($params['try'] > 5) {
                throw new WrongConfigurationException($this->translator->trans('wait_for_invitation.lasts_too_long'));
            }

            $job = $this->jobFactory->create(Job::SERVICE_QUEUE);
            $job->addTask('waitForInvitation', ['try' => $params['try'] + 1]);
            $this->jobFactory->enqueue($job, $params['try'] * 60);

            return [
                'status' => Job::STATUS_ERROR,
                'error' => $this->translator->trans('wait_for_invitation.not_yet_ready')
            ];

        }

        return [];
    }
}
