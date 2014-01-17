<?php
/**
 * DeleteFilter.php
 *
 * @author: Miroslav Čillík <miro@keboola.com>
 * @created: 30.4.13
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;

class DeleteFilter extends AbstractJob {

	function run($job, $params)
	{
		$gdWriteStartTime = date('c');

		$this->_checkParams($params, array(
			'uri'
		));

		try {
			$bucketAttributes = $this->configuration->bucketAttributes();
			$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
			try {
				$this->restApi->deleteFilter($params['uri']);
			} catch (RestApiException $e) {
				$mes = json_decode($e->getMessage(), true);

				var_dump($mes);

				if ($mes['error']['errorClass'] != 'GDC::Exception::NotFound') {
					throw new RestApiException($e->getMessage(), $e->getCode(), $e);
				}
			}

			$this->configuration->deleteFilter($params['uri']);

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
