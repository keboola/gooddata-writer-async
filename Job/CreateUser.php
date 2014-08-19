<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\JobProcessException;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\GoodData\UserAlreadyExistsException;

class CreateUser extends AbstractJob
{
	/**
	 * required: email, password, firstName, lastName
	 * optional: ssoProvider
	 */
	public function run($job, $params, RestApi $restApi)
	{
		$this->checkParams($params, array('email', 'password', 'firstName', 'lastName'));
		$params['email'] = strtolower($params['email']);

		$gdWriteStartTime = date('c');
		$restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);
		$ssoProvider = empty($params['ssoProvider']) ? $this->appConfiguration->gd_ssoProvider : $params['ssoProvider'];
		$alreadyExists = false;
		try {
			$userId = $restApi->createUser($this->getDomainUser()->domain, $params['email'], $params['password'],
				$params['firstName'], $params['lastName'], $ssoProvider);
		} catch (UserAlreadyExistsException $e) {
			$userId = $e->getMessage();
			$alreadyExists = true;
			if (!$userId) {
				throw new JobProcessException($this->translator->trans('error.user.in_other_domain'));
			}
		}

		$this->configuration->saveUser($params['email'], $userId);
		if (!$alreadyExists) {
			$this->sharedConfig->saveUser($job['projectId'], $job['writerId'], $userId, $params['email']);
		}

		$this->logEvent('createUser', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $restApi->getLogPath());
		return array(
			'uid' => $userId,
			'gdWriteStartTime' => $gdWriteStartTime,
			'alreadyExists' => $alreadyExists
		);
	}
}