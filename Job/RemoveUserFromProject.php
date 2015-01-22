<?php
/**
 * @author Erik Zigo <erik@keboola.com>
 * @date 2013-07-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\GoodData\RestApi;

class RemoveUserFromProject extends AbstractJob
{

	public function prepare($params)
	{
		$this->checkParams($params, array('writerId', 'pid', 'email'));
		$this->checkWriterExistence($params['writerId']);
		if (!$this->configuration->getProject($params['pid'])) {
			throw new WrongParametersException($this->translator->trans('parameters.pid_not_configured'));
		}
		if (!$this->configuration->isProjectUser($params['email'], $params['pid'])) {
			throw new WrongParametersException($this->translator->trans('parameters.email_not_configured'));
		}
		$this->configuration->checkProjectsTable();
		$this->configuration->checkProjectUsersTable();

		return array(
			'pid' => $params['pid'],
			'email' => $params['email']
		);
	}

	/**
	 * required: pid, email
	 * optional:
	 */
	public function run($job, $params, RestApi $restApi)
	{
		$this->checkParams($params, array('pid', 'email'));
		$params['email'] = strtolower($params['email']);

		$restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);

		if (!$this->configuration->isProjectUser($params['email'], $params['pid'])) {
			throw new WrongParametersException($this->translator->trans('parameters.email_not_configured_in_project'));
		}

		$userId = false;
		$user = $this->configuration->getUser($params['email']);
		if ($user && $user['uid']) {
			$userId = $user['uid'];
		}

		// find user in domain
		if (!$userId) {
			$userId = $restApi->userId($params['email'], $this->getDomainUser()->domain);

			if ($userId)
				$this->configuration->saveUser($params['email'], $userId);
			//@TODO save user invite to configuration if user came from invitation
		}

		// find user in project (maybe invited)
		if (!$userId) {
			$userId = $restApi->userIdByProject($params['email'], $params['pid']);
		}

		if ($userId) {
			$restApi->removeUserFromProject($userId, $params['pid']);
		}

		// cancel possible invitations
		$restApi->cancelInviteUserToProject($params['email'], $params['pid']);

		$this->configuration->removeProjectUserInvite($params['pid'], $params['email']);
		$this->configuration->removeProjectUserAdd($params['pid'], $params['email']);

		return array();
	}
}
