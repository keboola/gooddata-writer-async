<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\JobProcessException;
use Keboola\GoodDataWriter\GoodData\UserAlreadyExistsException;

class CreateUser extends AbstractJob
{
	public function run($job, $params)
	{
		$this->checkParams($params, array('email', 'password', 'firstName', 'lastName'));

		$gdWriteStartTime = date('c');
		$this->restApi->login($this->domainUser->username, $this->domainUser->password);
		$ssoProvider = empty($params['ssoProvider']) ? $this->appConfiguration->gd_ssoProvider : $params['ssoProvider'];
		$alreadyExists = false;
		try {
			$userId = $this->restApi->createUser($this->appConfiguration->gd_domain, $params['email'], $params['password'],
				$params['firstName'], $params['lastName'], $ssoProvider);
		} catch (UserAlreadyExistsException $e) {
			$userId = $e->getMessage();
			$alreadyExists = true;
			if (!$userId) {
				throw new JobProcessException(sprintf("User '%s' already exists and belongs to other domain", $params['email']));
			}
		}

		$this->configuration->saveUser($params['email'], $userId);
		$this->sharedConfig->saveUser($userId, $params['email'], $job);

		$this->logEvent('createUser', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $this->restApi->getLogPath());
		return array(
			'uid' => $userId,
			'gdWriteStartTime' => $gdWriteStartTime,
			'alreadyExists' => $alreadyExists
		);
	}
}