<?php
/**
 * @author Miroslav Cillik <miro@keboola.com>
 * @date 2013-04-22
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;

class createFilter extends GenericJob
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
			'name',
			'attribute',
			'element',
			'pid',
			'operator'
		));

		try {
			$this->restApi->login($mainConfig['username'], $mainConfig['password']);

			$filterUri = $this->restApi->createFilter(
				$params['name'],
				$params['attribute'],
				$params['element'],
				$params['operator'],
				$params['pid']
			);

			$this->configuration->saveFilterToConfiguration(
				$params['name'],
				$params['attribute'],
				$params['element'],
				$params['operator'],
				$filterUri
			);

			return $this->_prepareResult($job['id'], array(
				'uri' => $filterUri,
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
