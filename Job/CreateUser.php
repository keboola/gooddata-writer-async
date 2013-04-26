<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;

class CreateUser extends GenericJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		if (empty($params['email'])) {
			throw new WrongConfigurationException("Parameter 'email' is missing");
		}
		if (empty($params['password'])) {
			throw new WrongConfigurationException("Parameter 'password' is missing");
		}
		if (empty($params['firstName'])) {
			throw new WrongConfigurationException("Parameter 'firstName' is missing");
		}
		if (empty($params['lastName'])) {
			throw new WrongConfigurationException("Parameter 'lastName' is missing");
		}

		$env = empty($params['dev']) ? 'prod' :'dev';
		$mainConfig = $this->mainConfig['gd'][$env];


		$gdWriteStartTime = date('c');
		try {
			$this->restApi->login($mainConfig['username'], $mainConfig['password']);
			$userId = $this->restApi->createUser($mainConfig['domain'], $params['email'], $params['password'],
				$params['firstName'], $params['lastName'], $mainConfig['sso_provider']);

			$this->configuration->saveUserToConfiguration($params['email'], $userId);
			$this->sharedConfig->saveUser($userId, $params['email'], $job);


			return $this->_prepareResult($job['id'], array(
				'uid' => $userId,
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