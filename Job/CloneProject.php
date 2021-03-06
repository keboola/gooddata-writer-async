<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\GoodData\RestApi;

class CloneProject extends AbstractJob
{

    public function prepare($params)
    {
        $this->checkParams($params, array('writerId'));
        $this->checkWriterExistence($params['writerId']);

        $bucketAttributes = $this->configuration->bucketAttributes();
        $this->configuration->checkProjectsTable();

        if (empty($params['accessToken'])) {
            $params['accessToken'] = $this->gdAccessToken;
        }
        if (empty($params['name'])) {
            $params['name'] = sprintf(Model::PROJECT_NAME_TEMPLATE, $this->gdProjectNamePrefix, $this->configuration->tokenInfo['owner']['name'], $this->configuration->writerId);
        }

        return array(
            'accessToken' => $params['accessToken'],
            'name' => $params['name'],
            'includeData' => empty($params['includeData']) ? 0 : 1,
            'includeUsers' => empty($params['includeUsers']) ? 0 : 1,
            'pidSource' => $bucketAttributes['gd']['pid']
        );
    }


    /**
     * required: accessToken, name, pidSource
     * optional: includeData, includeUsers
     */
    public function run($job, $params, RestApi $restApi)
    {
        $this->checkParams($params, array('accessToken', 'name', 'pidSource'));

        $bucketAttributes = $this->configuration->bucketAttributes();

        // Check access to source project
        $restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
        $restApi->getProject($bucketAttributes['gd']['pid']);

        $restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);
        // Get user uri if not set
        if (empty($bucketAttributes['gd']['uid'])) {
            $userId = $restApi->userId($bucketAttributes['gd']['username'], $this->getDomainUser()->domain);
            $this->configuration->updateBucketAttribute('gd.uid', $userId);
            $bucketAttributes['gd']['uid'] = $userId;
        }
        $projectPid = $restApi->createProject($params['name'], $params['accessToken'], json_encode(array(
            'projectId' => $this->configuration->projectId,
            'writerId' => $this->configuration->writerId,
            'main' => false
        )));
        $restApi->cloneProject(
            $bucketAttributes['gd']['pid'],
            $projectPid,
            empty($params['includeData']) ? 0 : 1,
            empty($params['includeUsers']) ? 0 : 1
        );
        $restApi->addUserToProject($bucketAttributes['gd']['uid'], $projectPid);

        $this->configuration->saveProject($projectPid);
        $this->sharedStorage->saveProject($job['projectId'], $job['writerId'], $projectPid);

        return array(
            'pid' => $projectPid
        );
    }
}
