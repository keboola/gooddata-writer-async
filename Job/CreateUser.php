<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\JobRunException,
	Keboola\GoodDataWriter\Exception\RestApiException,
	Keboola\GoodDataWriter\Exception\UnauthorizedException;

class CreateUser extends GenericJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws JobRunException
	 * @return array
	 */
	public function run($job, $params)
	{
		if (empty($params['email'])) {
			throw new JobRunException("Parameter 'email' is missing");
		}
		if (empty($params['password'])) {
			throw new JobRunException("Parameter 'password' is missing");
		}
		if (empty($params['firstName'])) {
			throw new JobRunException("Parameter 'firstName' is missing");
		}
		if (empty($params['lastName'])) {
			throw new JobRunException("Parameter 'lastName' is missing");
		}

		$env = empty($params['dev']) ? 'prod' :'dev';
		$mainConfig = $this->mainConfig['gd'][$env];


		$gdWriteStartTime = date('c');
		try {
			$this->restApi->login($mainConfig['username'], $mainConfig['password']);
			$userUri = $this->restApi->createUserInDomain($mainConfig['domain'], $params['email'], $params['password'],
				$params['firstName'], $params['lastName'], $mainConfig['sso_provider']);

			$this->configuration->saveUserToConfiguration($params['email'], $userUri);
			$this->sharedConfig->saveUser($userUri, $params['email'], $job);


			return $this->_prepareResult($job['id'], array(
				'uri' => $userUri,
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->restApi->callsLog());

		} catch (UnauthorizedException $e) {
			throw new JobRunException('Login failed');
		} catch (RestApiException $e) {
			return $this->_prepareResult($job['id'], array(
				'status' => 'error',
				'error' => $e->getMessage(),
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->restApi->callsLog());
		}
	}
}