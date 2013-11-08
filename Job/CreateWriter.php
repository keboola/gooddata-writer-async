<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\GoodDataWriter\GoodData\UnauthorizedException;

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
		if (empty($params['accessToken'])) {
			throw new WrongConfigurationException("Parameter accessToken is missing");
		}
		if (empty($params['projectName'])) {
			throw new WrongConfigurationException("Parameter projectName is missing");
		}

        $this->configuration->updateTablesConfiguration();


		$gdWriteStartTime = date('c');
		$username = sprintf($this->mainConfig['gd']['user_email'], $job['projectId'], $job['writerId'] . '-' . uniqid());
		$password = md5(uniqid());

		$this->restApi->setCredentials($this->mainConfig['gd']['username'], $this->mainConfig['gd']['password']);
		try {
			try {
				$projectPid = $this->restApi->createProject($params['projectName'], $params['accessToken']);
			} catch (RestApiException $e) {
				throw new WrongConfigurationException('Project creation failed: ' . $e->getMessage());
			}

			$userId = $this->restApi->createUser($this->mainConfig['gd']['domain'], $username, $password, 'KBC', 'Writer', $this->mainConfig['gd']['sso_provider']);
			$this->restApi->addUserToProject($userId, $projectPid, RestApi::$userRoles['admin']);
		} catch (UnauthorizedException $e) {
			throw new \Exception('Create writer auth error', 500, $e);
		}

		// Save data to configuration bucket
		$this->configuration->updateWriter('gd.pid', $projectPid);
		$this->configuration->updateWriter('gd.username', $username);
		$this->configuration->updateWriter('gd.password', $password, true);
		$this->configuration->updateWriter('gd.uid', $userId);


		$this->sharedConfig->saveProject($projectPid, $params['accessToken'], $this->restApi->apiUrl, $job);
		$this->sharedConfig->saveUser($userId, $username, $job);


		return $this->_prepareResult($job['id'], array(
			'uid' => $userId,
			'pid' => $projectPid,
			'gdWriteStartTime' => $gdWriteStartTime
		), $this->restApi->callsLog());
	}
}