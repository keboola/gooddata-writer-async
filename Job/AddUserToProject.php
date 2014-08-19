<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\GoodData\RestApi;

class AddUserToProject extends AbstractJob
{
	/**
	 * required: pid, email, role
	 * optional: createUser
	 */
	public function run($job, $params, RestApi $restApi)
	{
		$this->checkParams($params, array('pid', 'email', 'role'));
		$params['email'] = strtolower($params['email']);

		$allowedRoles = array_keys(RestApi::$userRoles);
		if (!in_array($params['role'], $allowedRoles)) {
			throw new WrongConfigurationException($this->translator->trans('role %1', array('%1' => implode(', ', $allowedRoles))));
		}

		$restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);

		$startTime = date('c');
		$userId = null;

		// Get user uri
		$user = $this->configuration->getUser($params['email']);

		if ($user && $user['uid']) {
			// user created by writer
			$userId = $user['uid'];
		} else {
			$userId = $restApi->userId($params['email'], $this->getDomainUser()->domain);
			if ($userId) {
				// user in domain
				$this->configuration->saveUser($params['email'], $userId);
			}
		}

		if (!$userId) {
			if (!empty($params['createUser'])) {
				// try create new user in domain
				$childJob = new CreateUser($this->configuration, $this->appConfiguration, $this->sharedConfig,
					$this->s3Client, $this->translator, $this->storageApiClient);

				$childParams = array(
					'email' => $params['email'],
					'firstName' => 'KBC',
					'lastName' => $params['email'],
					'password' => md5(uniqid() . str_repeat($params['email'], 2)),
				);

				$result = $childJob->run($job, $childParams, $restApi);
				if (!empty($result['uid'])) {
					$userId = $result['uid'];
				}
			}
		}

		if ($userId) {
			$restApi->addUserToProject($userId, $params['pid'], RestApi::$userRoles[$params['role']]);

			$this->configuration->saveProjectUser($params['pid'], $params['email'], $params['role']);
		} else {
			$restApi->inviteUserToProject($params['email'], $params['pid'], RestApi::$userRoles[$params['role']]);

			$this->configuration->saveProjectInvite($params['pid'], $params['email'], $params['role']);
		}

		$this->logEvent('addUserToProject', array(
			'duration' => time() - strtotime($startTime)
		), $restApi->getLogPath());
		return array();
	}
}
