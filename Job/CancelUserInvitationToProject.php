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

class CancelUserInvitationToProject extends AbstractJob
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
		if (empty($params['email'])) {
			throw new WrongConfigurationException("Parameter 'email' is missing");
		}
		$this->configuration->checkGoodDataSetup();

		if (empty($params['pid'])) {
			if (empty($this->configuration->bucketInfo['gd']['pid'])) {
				throw new WrongConfigurationException("Parameter 'pid' is missing and writer does not have primary project");
			}
			$params['pid'] = $this->configuration->bucketInfo['gd']['pid'];
		}

		$gdWriteStartTime = date('c');
		try {
			$this->restApi->login($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);

			if (!$this->configuration->isProjectUser($params['email'], $params['pid'])) {
				throw new WrongParametersException(sprintf("Project user '%s' is not configured for the writer", $params['email']));
			}

			$this->restApi->cancelInviteUserToProject($params['email'], $params['pid']);

			if (!$this->_parentJob) {
				$childJob = $this->_createChildJob('removeUserFromProject');

				$childParams = array(
					'email' => $params['email'],
					'pid' => $params['pid'],
				);

				$result = $childJob->run($job, $childParams);
				if (empty($result['status'])) $result['status'] = 'success';

				if ($result['status'] != 'success')
					return $result;
			}

			$this->configuration->removeProjectUserInviteFromConfiguration($params['pid'], $params['email']);

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