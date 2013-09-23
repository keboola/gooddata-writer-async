<?php
/**
 * @author Miroslav Cillik <miro@keboola.com>
 * @date 2013-04-24
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;

class assignFiltersToUser extends GenericJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		$env = empty($params['dev']) ? 'prod' :'dev';
		$mainConfig = $this->mainConfig['gd'][$env];

		$gdWriteStartTime = date('c');

		$this->_checkParams($params, array(
			'filters',
			'userEmail',
			'pid'
		));

		if (!is_array($params['filters'])) {
			throw new WrongConfigurationException("Parameter 'filters' must be an array.");
		}

		$user = $this->configuration->getUser($params['userEmail']);
		$filterUris = array();
		foreach ($params['filters'] as $name) {
			$filter = $this->configuration->getFilter($name);
			$filterUris[] = $filter['uri'];
		}

		try {
			$this->restApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
			$this->restApi->assignFiltersToUser($filterUris, $user['uid'], $params['pid']);

			$this->configuration->saveFilterUserToConfiguration($filterUris, $params['userEmail']);

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
