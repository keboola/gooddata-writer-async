<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\GoodData\RestApi;

class CloneProject extends AbstractJob
{

	public function prepare($params)
	{
		$this->checkParams($params, array('writerId'));
		$this->checkWriterExistence($params['writerId']);
		$this->configuration->checkBucketAttributes();
		$this->configuration->checkProjectsTable();

		if (empty($params['accessToken'])) {
			$params['accessToken'] = $this->appConfiguration->gd_accessToken;
		}
		if (empty($params['name'])) {
			$params['name'] = sprintf($this->appConfiguration->gd_projectNameTemplate, $this->configuration->tokenInfo['owner']['name'], $this->configuration->writerId);
		}

		$bucketAttributes = $this->configuration->bucketAttributes();
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
		$this->configuration->checkBucketAttributes($bucketAttributes);

		$gdWriteStartTime = date('c');
		// Check access to source project
		$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$restApi->getProject($bucketAttributes['gd']['pid']);

		$restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);
		// Get user uri if not set
		if (empty($bucketAttributes['gd']['uid'])) {
			$userId = $restApi->userId($bucketAttributes['gd']['username'], $this->getDomainUser()->domain);
			$this->configuration->updateWriter('gd.uid', $userId);
			$bucketAttributes['gd']['uid'] = $userId;
		}
		$projectPid = $restApi->createProject($params['name'], $params['accessToken'], json_encode(array(
			'projectId' => $this->configuration->projectId,
			'writerId' => $this->configuration->writerId,
			'main' => false
		)));
		$restApi->cloneProject($bucketAttributes['gd']['pid'], $projectPid,
			empty($params['includeData']) ? 0 : 1, empty($params['includeUsers']) ? 0 : 1);
		$restApi->addUserToProject($bucketAttributes['gd']['uid'], $projectPid);

		$this->configuration->saveProject($projectPid);
		$this->sharedConfig->saveProject($job['projectId'], $job['writerId'], $projectPid);

		$this->logEvent('cloneProject', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $restApi->getLogPath());
		return array(
			'pid' => $projectPid,
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}