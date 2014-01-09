<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;
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
//		if (empty($params['pid'])) {
//			throw new WrongConfigurationException("Parameter 'pid' is missing");
//		}
		if (empty($params['email'])) {
			throw new WrongConfigurationException("Parameter 'email' is missing");
		}
		if (empty($params['role'])) {
			throw new WrongConfigurationException("Parameter 'role' is missing");
		}
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

		$this->restApi->setCredentials($this->mainConfig['gd']['username'], $this->mainConfig['gd']['password']);

		$gdWriteStartTime = date('c');
		try {
			$userId = null;

			// Get user uri
			$user = $this->configuration->getUser($params['email']);

			if ($user && $user['uid']) {
				// user created by writer
				$userId = $user['uid'];
			} else {
				$userId = $this->restApi->userId($params['email'], $this->mainConfig['gd']['domain']);
				if ($userId) {
					// user in domain
					$this->configuration->saveUser($params['email'], $userId);
				}
			}

			if (!$userId) {
				if (!empty($params['createUser'])) {
					// try create new user in domain
					$childJob = new CreateUser($this->configuration, $this->mainConfig, $this->sharedConfig, $this->restApi, $this->s3Client);

					$childParams = array(
						'email' => $params['email'],
						'firstName' => 'KBC',
						'lastName' => $params['email'],
						'password' => md5(uniqid() . str_repeat($params['email'], 2)),
					);

					$result = $childJob->run($job, $childParams);
					if (empty($result['status'])) $result['status'] = 'success';

					if ($result['status'] == 'success')
						$userId = $result['uid'];
				}
			}

			if ($userId) {
				$this->restApi->addUserToProject($userId, $params['pid'], RestApi::$userRoles[$params['role']]);

				$this->configuration->saveProjectUser($params['pid'], $params['email'], $params['role']);
			} else {
				$this->restApi->inviteUserToProject($params['email'], $params['pid'], RestApi::$userRoles[$params['role']]);

				$this->configuration->saveProjectInvite($params['pid'], $params['email'], $params['role']);
			}

			return $this->_prepareResult($job['id'], array(
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->restApi->callsLog());

		} catch (UnauthorizedException $e) {
			throw new WrongConfigurationException('Rest API Login failed');
		} catch (RestApiException $e) {
			return $this->_prepareResult($job['id'], array(
				'status' => 'error',
				'error' => $e->getMessage(),
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->restApi->callsLog());
		}
	}
}
