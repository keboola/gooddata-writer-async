<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\Job\Metadata\Job;

class CloneProject extends AbstractTask
{

    public function prepare($params)
    {
        $this->checkParams($params, ['writerId']);
        $this->checkWriterExistence($params['writerId']);

        $bucketAttributes = $this->configuration->getBucketAttributes();
        $this->configuration->checkProjectsTable();

        if (empty($params['accessToken'])) {
            $params['accessToken'] = $this->gdAccessToken;
        }
        if (empty($params['name'])) {
            $params['name'] = sprintf(Model::PROJECT_NAME_TEMPLATE, $this->gdProjectNamePrefix, $this->configuration->tokenInfo['owner']['name'], $this->configuration->writerId);
        }

        return [
            'accessToken' => $params['accessToken'],
            'name' => $params['name'],
            'includeData' => empty($params['includeData']) ? 0 : 1,
            'includeUsers' => empty($params['includeUsers']) ? 0 : 1,
            'pidSource' => $bucketAttributes['gd']['pid']
        ];
    }


    /**
     * required: accessToken, name, pidSource
     * optional: includeData, includeUsers
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);

        $this->checkParams($params, ['accessToken', 'name', 'pidSource']);

        $bucketAttributes = $this->configuration->getBucketAttributes();

        // Check access to source project
        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
        $this->restApi->getProject($bucketAttributes['gd']['pid']);

        $this->restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);
        // Get user uri if not set
        if (empty($bucketAttributes['gd']['uid'])) {
            $userId = $this->restApi->userId($bucketAttributes['gd']['username'], $this->getDomainUser()->domain);
            $this->configuration->updateBucketAttribute('gd.uid', $userId);
            $bucketAttributes['gd']['uid'] = $userId;
        }
        $projectPid = $this->restApi->createProject($params['name'], $params['accessToken'], json_encode([
            'projectId' => $this->configuration->projectId,
            'writerId' => $this->configuration->writerId,
            'main' => false
        ]));
        $this->restApi->cloneProject(
            $bucketAttributes['gd']['pid'],
            $projectPid,
            empty($params['includeData']) ? 0 : 1,
            empty($params['includeUsers']) ? 0 : 1
        );
        $this->restApi->addUserToProject($bucketAttributes['gd']['uid'], $projectPid);

        $this->configuration->saveProject($projectPid);
        $this->sharedStorage->saveProject($this->configuration->projectId, $this->configuration->writerId, $projectPid);

        return [
            'pid' => $projectPid
        ];
    }
}
