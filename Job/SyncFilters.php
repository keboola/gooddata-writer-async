<?php
/**
 * SyncFilters.php
 *
 * @author: Miroslav Čillík <miro@keboola.com>
 * @created: 3.5.13
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;

class SyncFilters extends GenericJob {

	function run($job, $params)
	{
		$env = empty($params['dev']) ? 'prod' :'dev';
		$mainConfig = $this->mainConfig['gd'][$env];

		$gdWriteStartTime = date('c');

		$this->_checkParams($params, array(
			'pid'
		));

		try {
			$this->restApi->login($mainConfig['username'], $mainConfig['password']);

			// Delete filters from GD project
			$gdFilters = $this->restApi->getFilters($params['pid']);
			foreach ($gdFilters as $gdf) {
				$this->restApi->deleteFilter($gdf['link']);
			}

			// Create filters
			foreach ($this->configuration->getFilters() as $f) {

				$filterUri = $this->restApi->createFilter(
					$f['name'],
					$f['attribute'],
					$f['element'],
					$f['operator'],
					$params['pid']
				);

				$this->configuration->updateFilters(
					$f['name'],
					$f['attribute'],
					$f['element'],
					$f['operator'],
					$filterUri
				);
			}

			// Assign filters to user
			foreach ($this->configuration->getUsers() as $u) {
				$filters = array();
				foreach ($this->configuration->getFiltersUsers() as $fu) {
					if ($fu['userEmail'] == $u['email']) {
						foreach ($this->configuration->getFilters() as $f) {
							if ($fu['filterName'] == $f['name']) {
								$filters[] = $f['uri'];
							}
						}
					}
				}

				$this->restApi->assignFiltersToUser($filters, $u['uid'], $params['pid']);
			}

			return $this->_prepareResult($job['id'], array(
				'filters'   => $this->configuration->getFilters(),
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
