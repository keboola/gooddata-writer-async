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
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		$this->checkParams($params, array('email', 'role'));

		$allowedRoles = array_keys(RestApi::$userRoles);
		if (!in_array($params['role'], $allowedRoles)) {
			throw new WrongConfigurationException("Parameter 'role' is not valid; it has to be one of: " . implode(', ', $allowedRoles));
		}

		$this->configuration->checkBucketAttributes();

		if (empty($params['pid'])) {
			$bucketAttributes = $this->configuration->bucketAttributes();
			if (empty($bucketAttributes['gd']['pid'])) {
				throw new WrongConfigurationException("Parameter 'pid' is missing and writer does not have primary project");
			}
			$params['pid'] = $bucketAttributes['gd']['pid'];
		}

		$this->restApi->login($this->appConfiguration->gd_username, $this->appConfiguration->gd_password);

		$gdWriteStartTime = date('c');
		$userId = null;

		// Get user uri
		$user = $this->configuration->getUser($params['email']);

		if ($user && $user['uid']) {
			// user created by writer
			$userId = $user['uid'];
		} else {
			$userId = $this->restApi->userId($params['email'], $this->appConfiguration->gd_domain);
			if ($userId) {
				// user in domain
				$this->configuration->saveUser($params['email'], $userId);
			}
		}

		if (!$userId) {
			if (!empty($params['createUser'])) {
				// try create new user in domain
				$childJob = new CreateUser($this->configuration, $this->appConfiguration, $this->sharedConfig,
					$this->restApi, $this->s3Client, $this->tempServiceFactory);

				$childParams = array(
					'email' => $params['email'],
					'firstName' => 'KBC',
					'lastName' => $params['email'],
					'password' => md5(uniqid() . str_repeat($params['email'], 2)),
				);

				$result = $childJob->run($job, $childParams);
				if (!empty($result['uid'])) {
					$userId = $result['uid'];
				}
			}
		}

		if ($userId) {
			$this->restApi->addUserToProject($userId, $params['pid'], RestApi::$userRoles[$params['role']]);

			$this->configuration->saveProjectUser($params['pid'], $params['email'], $params['role']);
		} else {
			$this->restApi->inviteUserToProject($params['email'], $params['pid'], RestApi::$userRoles[$params['role']]);

			$this->configuration->saveProjectInvite($params['pid'], $params['email'], $params['role']);
		}

		$this->logEvent('addUserToProject', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $this->restApi->getLogPath());
		return array(
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}
