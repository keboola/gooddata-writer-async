<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\GoodData\RestApi;

class CreateWriter extends AbstractJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws \Exception
	 * @throws \Keboola\GoodDataWriter\Exception\WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		$this->checkParams($params, array('accessToken', 'projectName'));

        $this->configuration->updateDataSetsFromSapi();

		$gdWriteStartTime = date('c');
		$username = sprintf($this->appConfiguration->gd_userEmailTemplate, $job['projectId'], $job['writerId'] . '-' . uniqid());
		$password = md5(uniqid());

		$this->restApi->login($this->appConfiguration->gd_username, $this->appConfiguration->gd_password);
		$projectPid = $this->restApi->createProject($params['projectName'], $params['accessToken']);

		$userId = $this->restApi->createUser($this->appConfiguration->gd_domain, $username, $password, 'KBC', 'Writer', $this->appConfiguration->gd_ssoProvider);
		$this->restApi->addUserToProject($userId, $projectPid, RestApi::$userRoles['admin']);

		// Save data to configuration bucket
		$this->configuration->updateWriter('gd.pid', $projectPid);
		$this->configuration->updateWriter('gd.username', $username);
		$this->configuration->updateWriter('gd.password', $password, true);
		$this->configuration->updateWriter('gd.uid', $userId);


		$this->sharedConfig->saveProject($projectPid, $params['accessToken'], $this->restApi->getApiUrl(), $job);
		$this->sharedConfig->saveUser($userId, $username, $job);


		$this->logEvent('createUser', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $this->restApi->getLogPath());
		return array(
			'uid' => $userId,
			'pid' => $projectPid,
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}