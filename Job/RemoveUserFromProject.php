<?php
/**
 * @author Erik Zigo <erik@keboola.com>
 * @date 2013-07-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\Exception\WrongParametersException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;
use Keboola\GoodDataWriter\GoodData\RestApi;

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
		if (empty($params['pid'])) {
			throw new WrongConfigurationException("Parameter 'pid' is missing");
		}
		if (empty($params['email'])) {
			throw new WrongConfigurationException("Parameter 'email' is missing");
		}

		$env = empty($params['dev']) ? 'prod' :'dev';
		$mainConfig = $this->mainConfig['gd'][$env];

		$gdWriteStartTime = date('c');
		try {
			// Get user uri
			$this->restApi->setCredentials($mainConfig['username'], $mainConfig['password']);

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
				$userId = $this->restApi->userId($params['email'], $mainConfig['domain']);

				if ($userId)
					$this->configuration->saveUserToConfiguration($params['email'], $userId);
			}

			// find user in project (maybe invited)
			if (!$userId) {
				$userId = $this->restApi->userIdByProject($params['email'], $params['pid']);
			}

			if (!$userId && !$this->_parentJob)
				throw new WrongConfigurationException("User is missing from configuration");

			if ($userId) {
				$this->restApi->removeUserFromProject($userId, $params['pid']);
			}

			// cancel possible invitations
			if (!$this->_parentJob) {
				$childJob = $this->_createChildJob('cancelUserInvitationToProject');

				$childParams = array(
					'email' => $params['email'],
					'pid' => $params['pid'],
				);

				$result = $childJob->run($job, $childParams);
				if (empty($result['status'])) $result['status'] = 'success';

				if ($result['status'] != 'success')
					return $result;
			}

			$this->configuration->removeProjectUserAddFromConfiguration($params['pid'], $params['email']);

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