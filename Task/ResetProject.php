<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 25.02.14
 * Time: 17:23
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\Exception\RestApiException;
use Keboola\GoodDataWriter\Writer\Job;

class ResetProject extends AbstractTask
{

    public function prepare($params)
    {
        $this->checkParams($params, ['writerId']);
        $this->checkWriterExistence($params['writerId']);

        return [
            'removeClones' => isset($params['removeClones'])? (bool)$params['removeClones'] : false
        ];
    }

    /**
     * required:
     * optional: removeClones, accessToken
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);
        $removeClones = isset($params['removeClones']) ? (bool)$params['removeClones'] : false;

        $projectName = sprintf(Model::PROJECT_NAME_TEMPLATE, $this->gdProjectNamePrefix, $this->configuration->tokenInfo['owner']['name'], $this->configuration->writerId);
        $accessToken = !empty($params['accessToken']) ? $params['accessToken'] : $this->gdAccessToken;
        $bucketAttributes = $this->configuration->bucketAttributes();

        $oldPid = $bucketAttributes['gd']['pid'];
        $userId = $bucketAttributes['gd']['uid'];

        $this->restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);
        $newPid = $this->restApi->createProject($projectName, $accessToken, json_encode([
            'projectId' => $this->configuration->projectId,
            'writerId' => $this->configuration->writerId,
            'main' => true
        ]));

        // All users from old project add to the new one with the same role
        $oldRoles = [];
        foreach ($this->restApi->usersInProject($oldPid) as $user) {
            $userEmail = $user['user']['content']['email'];
            if ($userEmail == $this->getDomainUser()->username) {
                continue;
            }

            $userId = RestApi::getUserId($user['user']['links']['self']);
            if (isset($user['user']['content']['userRoles'])) {
                foreach ($user['user']['content']['userRoles'] as $roleUri) {
                    if (!in_array($roleUri, array_keys($oldRoles))) {
                        $role = $this->restApi->get($roleUri);
                        if (isset($role['projectRole']['meta']['identifier'])) {
                            $oldRoles[$roleUri] = $role['projectRole']['meta']['identifier'];
                        }
                    }
                    if (isset($oldRoles[$roleUri])) {
                        try {
                            $this->restApi->addUserToProject($userId, $newPid, $oldRoles[$roleUri]);
                        } catch (RestApiException $e) {
                            $this->restApi->inviteUserToProject($userEmail, $newPid, $oldRoles[$roleUri]);
                        }
                    }
                }
            }

            $this->restApi->disableUserInProject($user['user']['links']['self'], $oldPid);
        }

        $this->sharedStorage->enqueueProjectToDelete($this->configuration->projectId, $this->configuration->writerId, $oldPid);

        if ($removeClones) {
            foreach ($this->configuration->getProjects() as $p) {
                if (empty($p['main'])) {
                    $this->restApi->disableUserInProject(RestApi::getUserUri($userId), $p['pid']);
                    $this->sharedStorage->enqueueProjectToDelete($this->configuration->projectId, $this->configuration->writerId, $p['pid']);
                }
            }
            $this->configuration->resetProjectsTable();
        }

        $this->configuration->updateBucketAttribute('gd.pid', $newPid);

        foreach ($this->configuration->getDataSets() as $dataSet) {
            $this->configuration->updateDataSetDefinition($dataSet['id'], [
                'isExported' => null
            ]);
        }
        foreach ($this->configuration->getDateDimensions() as $dimension) {
            $this->configuration->setDateDimensionIsExported($dimension['name'], false);
        }

        return [
            'newPid' => $newPid
        ];
    }
}
