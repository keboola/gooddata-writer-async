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

class DeleteFilter extends GenericJob {

	function run($job, $params)
	{
		$env = empty($params['dev']) ? 'prod' :'dev';
		$mainConfig = $this->mainConfig['gd'][$env];

		$gdWriteStartTime = date('c');
		try {
			$this->restApi->login($mainConfig['username'], $mainConfig['password']);
			$this->restApi->deleteFilter($params['uri']);

			$this->configuration->deleteFilterFromConfiguration($params['uri']);
//			$this->sharedConfig->saveFilter();

			return $this->_prepareResult($job['id'], array(
				'uri' => 'todo',
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
