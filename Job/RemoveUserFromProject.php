<?php
/**
 * @author Erik Zigo <erik@keboola.com>
 * @date 2013-07-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\Exception\WrongParametersException;

class RemoveUserFromProject extends AbstractJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongParametersException
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		$this->checkParams($params, array('pid', 'email'));

		$this->restApi->login($this->appConfiguration->gd_username, $this->appConfiguration->gd_password);

		$gdWriteStartTime = date('c');
		if (!$this->configuration->isProjectUser($params['email'], $params['pid'])) {
			throw new WrongParametersException(sprintf("Project user '%s' is not configured for the writer", $params['email']));
		}

		$userId = false;
		$user = $this->configuration->getUser($params['email']);
		if ($user && $user['uid']) {
			$userId = $user['uid'];
		}

		// find user in domain
		if (!$userId) {
			$userId = $this->restApi->userId($params['email'], $this->appConfiguration->gd_domain);

			if ($userId)
				$this->configuration->saveUser($params['email'], $userId);
			//@TODO save user invite to configuration if user came from invitation
		}

		// find user in project (maybe invited)
		if (!$userId) {
			$userId = $this->restApi->userIdByProject($params['email'], $params['pid']);
		}

		if ($userId) {
			$this->restApi->removeUserFromProject($userId, $params['pid']);
		}

		// cancel possible invitations
		$this->restApi->cancelInviteUserToProject($params['email'], $params['pid']);

		$this->configuration->removeProjectUserInvite($params['pid'], $params['email']);
		$this->configuration->removeProjectUserAdd($params['pid'], $params['email']);

		$this->logEvent('removeUserFromProject', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $this->restApi->getLogPath());
		return array(
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}
