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

class createFilter extends AbstractJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
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
			$bucketInfo = $this->configuration->bucketInfo();
			$this->restApi->setCredentials($bucketInfo['gd']['username'], $bucketInfo['gd']['password']);

			$filterUri = $this->restApi->createFilter(
				$params['name'],
				$attrId,
				$params['element'],
				$params['operator'],
				$params['pid']
			);

			$this->configuration->saveFilter(
				$params['name'],
				$params['attribute'],
				$params['element'],
				$params['operator'],
				$filterUri
			);

			$this->configuration->saveFiltersProjects($params['name'], $params['pid']);

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
