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

class AddUserToProject extends GenericJob
{
	/**
	 * @param $job
	 * @param $params
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
		if (empty($params['role'])) {
			throw new WrongConfigurationException("Parameter 'role' is missing");
		}

		$env = empty($params['dev']) ? 'prod' :'dev';
		$mainConfig = $this->mainConfig['gd'][$env];


		$gdWriteStartTime = date('c');
		try {
			// Get user uri
			$this->restApi->login($mainConfig['username'], $mainConfig['password']);

			$user = $this->configuration->user($params['email']);
			if (!$user) {
				throw new WrongConfigurationException("User is missing from configuration");
			}

			if ($user['uri']) {
				$userUri = $user['uri'];
			} else {
				$userUri = $this->restApi->userUri($params['email'], $mainConfig['domain']);
				$this->configuration->saveUserToConfiguration($params['email'], $userUri);
				if (!$userUri) {
					throw new WrongConfigurationException(sprintf("User '%s' does not exist in domain", $params['email']));
				}
			}

			$this->restApi->addUserToProject($userUri, $params['pid'], RestApi::$userRoles[$params['role']]);

			$this->configuration->saveProjectUserToConfiguration($params['pid'], $params['email'], $params['role']);


			return $this->_prepareResult($job['id'], array(
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->restApi->callsLog());

		} catch (UnauthorizedException $e) {
			throw new WrongConfigurationException('Login failed');
		} catch (RestApiException $e) {
			return $this->_prepareResult($job['id'], array(
				'status' => 'error',
				'error' => $e->getMessage(),
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->restApi->callsLog());
		}
	}
}