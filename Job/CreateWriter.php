<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;

class CreateWriter extends GenericJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		if (empty($params['accessToken'])) {
			throw new WrongConfigurationException("Parameter accessToken is missing");
		}
		if (empty($params['projectName'])) {
			throw new WrongConfigurationException("Parameter projectName is missing");
		}

		$env = empty($params['dev']) ? 'prod' :'dev';
		$mainConfig = $this->mainConfig['gd'][$env];


		$gdWriteStartTime = date('c');
		$username = sprintf($mainConfig['user_email'], $job['projectId'], $job['writerId']);
		$password = md5(uniqid());

		$this->restApi->login($mainConfig['username'], $mainConfig['password']);
		$projectPid = $this->restApi->createProject($params['projectName'], $params['accessToken']);
		$userUri = $this->restApi->createUserInDomain($mainConfig['domain'], $username, $password, 'KBC', 'Writer', $mainConfig['sso_provider']);
		$this->restApi->addUserToProject($userUri, $projectPid);

		// Save data to configuration bucket
		$this->configuration->setBucketAttribute('gd.pid', $projectPid);
		$this->configuration->setBucketAttribute('gd.username', $username);
		$this->configuration->setBucketAttribute('gd.password', $password, true);
		$this->configuration->setBucketAttribute('gd.userUri', $userUri);


		$this->sharedConfig->saveProject($projectPid, $params['accessToken'], $this->restApi->apiUrl, $job);
		$this->sharedConfig->saveUser($userUri, $username, $job);


		return $this->_prepareResult($job['id'], array(
			'pid' => $projectPid,
			'gdWriteStartTime' => $gdWriteStartTime
		), $this->restApi->callsLog());
	}
}