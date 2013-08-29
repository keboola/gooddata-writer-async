<?php
/**
 * @author Miroslav Cillik <miro@keboola.com>
 * @date 2013-04-22
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;
use Keboola\GoodDataWriter\Exception\WrongParametersException;

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

		$attrId = $this->configuration->translateAttributeName($params['attribute']);

		try {
			$this->restApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);

			$filterUri = $this->restApi->createFilter(
				$params['name'],
				$attrId,
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

			$this->configuration->saveFiltersProjectsToConfiguration($params['name'], $params['pid']);

			return $this->_prepareResult($job['id'], array(
				'uri' => $filterUri,
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->restApi->callsLog());

		} catch (UnauthorizedException $e) {
			throw new WrongConfigurationException('Login failed');
		} catch (WrongParametersException $e) {
			throw new WrongConfigurationException($e->getMessage());
		} catch (RestApiException $e) {
			return $this->_prepareResult($job['id'], array(
				'status' => 'error',
				'error' => $e->getMessage(),
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->restApi->callsLog());
		}
	}
}
