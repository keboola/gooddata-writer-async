<?php
/**
 * Created by PhpStorm.
 * User: mirocillik
 * Date: 19/11/13
 * Time: 15:00
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;

class CreateObject  extends AbstractJob
{

	function run($job, $params)
	{
		$this->_checkParams($params, array('pid', 'object'));

		$objectId = isset($params['objectId'])?$params['objectId']:null;
		$definition = $params['object'];
		$pid = $params['pid'];

		$gdWriteStartTime = date('c');
		try {
			$this->restApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
			$response = $this->restApi->postObject($pid, $definition, $objectId);

			return $this->_prepareResult($job['id'], array(
				'objectId' => $response['uri'],
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