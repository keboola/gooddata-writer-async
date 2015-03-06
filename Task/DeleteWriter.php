<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\Exception\RestApiException;
use Keboola\GoodDataWriter\Writer\Job;

class DeleteWriter extends AbstractTask
{

    public function prepare($params)
    {
        $this->checkParams($params, ['writerId']);
        $this->checkWriterExistence($params['writerId']);

        //@TODO cancel jobs
        //$this->jobStorage->cancelJobs($this->configuration->projectId, $this->configuration->writerId);
        $this->sharedStorage->deleteWriter($this->configuration->projectId, $this->configuration->writerId);

        return [];
    }

    /**
     * required:
     * optional:
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);
        if (!$this->configuration->bucketId) {
            throw new WrongConfigurationException('Writer has been already deleted.');
        }

        $this->restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);

        foreach ($this->sharedStorage->getProjects($this->configuration->projectId, $this->configuration->writerId) as $project) {
            if (!$project['keep_on_removal']) {
                if ($this->configuration->testingWriter) {
                    try {
                        $this->restApi->dropProject($project['pid']);
                    } catch (RestApiException $e) {
                        // Ignore, project may have been already deleted
                    }
                    $this->sharedStorage->markProjectsDeleted([$project['pid']]);
                } else {
                    $this->sharedStorage->enqueueProjectToDelete($this->configuration->projectId, $this->configuration->writerId, $project['pid']);

                    // Disable users in project
                    try {
                        $projectUsers = $this->restApi->get(sprintf('/gdc/projects/%s/users', $project['pid']));
                        foreach ($projectUsers['users'] as $user) {
                            if ($user['user']['content']['email'] != $this->getDomainUser()->username) {
                                $this->restApi->disableUserInProject($user['user']['links']['self'], $project['pid']);
                            }
                        }
                    } catch (RestApiException $e) {
                        // Ignore, project may have been already deleted
                    }
                }
            }
        }

        // Remove only users created by Writer
        foreach ($this->sharedStorage->getUsers($this->configuration->projectId, $this->configuration->writerId) as $user) {
            if (strpos($user['email'], $this->gdUsernameDomain) !== false) {
                if ($this->configuration->testingWriter) {
                    $this->restApi->dropUser($user['uid']);
                    $this->sharedStorage->markUsersDeleted([$user['uid']]);
                } else {
                    $this->sharedStorage->enqueueUserToDelete($this->configuration->projectId, $this->configuration->writerId, $user['uid']);
                }
            }
        }

        $this->configuration->deleteBucket();

        return [];
    }
}
