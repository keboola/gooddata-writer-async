<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

class CloneProject extends AbstractJob
{
	/**
	 * required: accessToken, projectName, pidSource
	 * optional: includeData, includeUsers
	 */
	public function run($job, $params)
	{
		$this->checkParams($params, array('accessToken', 'projectName', 'pidSource'));

		$this->configuration->checkBucketAttributes();
		$bucketAttributes = $this->configuration->bucketAttributes();

		$gdWriteStartTime = date('c');
		// Check access to source project
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$this->restApi->getProject($bucketAttributes['gd']['pid']);

		$this->restApi->login($this->domainUser->username, $this->domainUser->password);
		// Get user uri if not set
		if (empty($bucketAttributes['gd']['uid'])) {
			$userId = $this->restApi->userId($bucketAttributes['gd']['username'], $this->appConfiguration->gd_domain);
			$this->configuration->updateWriter('gd.uid', $userId);
			$bucketAttributes['gd']['uid'] = $userId;
		}
		$projectPid = $this->restApi->createProject($params['projectName'], $params['accessToken'], json_encode(array(
			'projectId' => $this->configuration->projectId,
			'writerId' => $this->configuration->writerId,
			'main' => false
		)));
		$this->restApi->cloneProject($bucketAttributes['gd']['pid'], $projectPid,
			empty($params['includeData']) ? 0 : 1, empty($params['includeUsers']) ? 0 : 1);
		$this->restApi->addUserToProject($bucketAttributes['gd']['uid'], $projectPid);

		$this->configuration->saveProject($projectPid);
		$this->sharedConfig->saveProject($job['projectId'], $job['writerId'], $projectPid);

		$this->logEvent('cloneProject', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $this->restApi->getLogPath());
		return array(
			'pid' => $projectPid,
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}