<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\JobRunException,
	Keboola\GoodDataWriter\Exception\RestApiException,
	Keboola\GoodDataWriter\Exception\UnauthorizedException;
use Keboola\GoodDataWriter\GoodData\RestApi;

class InviteUserToProject extends GenericJob
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
		if (empty($params['role'])) {
			throw new JobRunException("Parameter 'role' is missing");
		}
		$this->configuration->checkGoodDataSetup();

		if (empty($params['pid'])) {
			if (empty($this->configuration->bucketInfo['gd']['pid'])) {
				throw new JobRunException("Parameter 'pid' is missing and writer does not have primary project");
			}
			$params['pid'] = $this->configuration->bucketInfo['gd']['pid'];
		}


		$gdWriteStartTime = date('c');
		try {
			$this->restApi->login($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
			$this->restApi->inviteUserToProject($params['email'], $params['pid'], RestApi::$userRoles[$params['role']]);

			$this->configuration->saveProjectUserToConfiguration($params['pid'], $params['email'], $params['role']);

			return $this->_prepareResult($job['id'], array(
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