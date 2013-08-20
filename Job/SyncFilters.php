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

		try {
			$this->restApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);

			$projects = $this->configuration->getProjects();
			if (isset($params['pid'])) {
				$projects = array($this->configuration->getProject($params['pid']));
			}

			foreach($projects as $p) {

				// Delete filters from project
				$gdFilters = $this->restApi->getFilters($p['pid']);
				foreach ($gdFilters as $gdf) {
					$this->restApi->deleteFilter($gdf['link']);
				}

				// get users in project
				foreach($this->configuration->getProjectUsers() as $pu) {

					if ($pu['pid'] == $p['pid']) {
						$user = $this->configuration->getUser($pu['email']);

						// get filters for user
						$filters = array();
						foreach($this->configuration->getFiltersUsers() as $fu) {
							if ($fu['userEmail'] == $user['email'] && $this->isFilterInProject($fu['filterName'], $p['pid'])) {

								// Create filter
								$filter = $this->configuration->getFilter($fu['filterName']);
								$attrId = $this->configuration->translateAttributeName($filter['attribute']);
								$elem = is_array(json_decode($filter['element'], true))?json_decode($filter['element'], true):$filter['element'];
								$filterUri = $this->restApi->createFilter(
									$filter['name'],
									$attrId,
									$elem,
									$filter['operator'],
									$p['pid']
								);

								// Update filter uri
								$this->configuration->updateFilters(
									$filter['name'],
									$filter['attribute'],
									$filter['element'],
									$filter['operator'],
									$filterUri
								);

								$filters[] = $filterUri;
							}
						}

						if (count($filters)) {
							$this->restApi->assignFiltersToUser($filters, $user['uid'], $p['pid']);
						}
					}
				}
			}

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

	protected function isFilterInProject($filterName, $pid) {
		foreach ($this->configuration->getFiltersProjects() as $fp) {
			if ($fp['filterName'] == $filterName && $fp['pid'] == $pid) {
				return true;
			}
		}
		return false;
	}
}
