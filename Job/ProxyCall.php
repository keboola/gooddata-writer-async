<?php
/**
 * Proxy Call Job
 *
 * User: mirocillik
 * Date: 19/11/13
 * Time: 15:00
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;

class ProxyCall  extends AbstractJob
{
	function run($job, $params)
	{
		$this->_checkParams($params, array('query', 'payload'));

		$gdWriteStartTime = date('c');
		try {
			$this->restApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
			$response = $this->restApi->post($params['query'], $params['payload']);

			return $this->_prepareResult($job['id'], array(
				'response' => $response,
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