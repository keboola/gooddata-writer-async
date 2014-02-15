<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;

class CreateUser extends AbstractJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		$this->checkParams($params, array('email', 'password', 'firstName', 'lastName'));

		$gdWriteStartTime = date('c');
		$this->restApi->login($this->appConfiguration->gd_username, $this->appConfiguration->gd_password);
		$ssoProvider = empty($params['ssoProvider']) ? $this->appConfiguration->gd_ssoProvider : $params['ssoProvider'];
		$userId = $this->restApi->createUser($this->appConfiguration->gd_domain, $params['email'], $params['password'],
			$params['firstName'], $params['lastName'], $ssoProvider);

		$this->configuration->saveUser($params['email'], $userId);
		$this->sharedConfig->saveUser($userId, $params['email'], $job);

		$this->logEvent('createUser', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $this->restApi->getLogPath());
		return array(
			'uid' => $userId,
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}