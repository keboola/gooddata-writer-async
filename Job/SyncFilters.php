<?php
/**
 * SyncFilters.php
 *
 * @author: Miroslav Čillík <miro@keboola.com>
 * @created: 3.5.13
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\GoodData\RestApi;

class SyncFilters extends AbstractJob
{

	public function prepare($params)
	{
		$this->checkParams($params, array('writerId'));
		$this->checkWriterExistence($params['writerId']);

		return array(
			'pid' => empty($params['pid'])? null : $params['pid']
		);
	}

	/**
	 * required:
	 * optional: pid
	 */
	function run($job, $params, RestApi $restApi)
	{
		$gdWriteStartTime = date('c');
		$bucketAttributes = $this->configuration->bucketAttributes();
		$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		// Delete all filters from project
		$gdFilters = $restApi->getFilters($params['pid']);
		foreach ($gdFilters as $gdf) {
			$restApi->deleteFilter($gdf['link']);
		}

		// Create filters
		$filterUris = array();
		$filtersToCreate = array();
		foreach ($this->configuration->getFiltersProjectsByPid($params['pid']) as $fp) {
			$filtersToCreate[] = $fp['filter'];
			$this->configuration->deleteFilterFromProject($fp['uri']);
		}
		foreach ($this->configuration->getFilters() as $f) if (in_array($f['name'], $filtersToCreate)) {
			$value = json_decode($f['value'], true);
			if (!is_array($value)) {
				$value = $f['value'];
			}
			$tableId = $this->configuration->getTableIdFromAttribute($f['attribute']);
			$tableDefinition = $this->configuration->getDataSet($tableId);
			$tableName = empty($tableDefinition['name'])? $tableId : $tableDefinition['name'];
			$attrName = substr($f['attribute'], strrpos($f['attribute'], '.') + 1);
			$attrId = Model::getAttributeId($tableName, $attrName);
			$filterUris[$f['name']] = $restApi->createFilter($f['name'], $attrId, $f['operator'], $value, $params['pid']);
			$this->configuration->saveFiltersProjects($filterUris[$f['name']], $f['name'], $params['pid']);
		}

		// Assign filters to users
		$filtersUsers = $this->configuration->getFiltersUsers();
		$filtersProjects = $this->configuration->getFiltersProjectsByPid($params['pid']);
		foreach($this->configuration->getProjectUsers($params['pid']) as $pu) {
			$user = $this->configuration->getUser($pu['email']);

			// get filters for user
			$filters = array();
			foreach($filtersUsers as $fu) if ($fu['email'] == $user['email']) {
				foreach ($filtersProjects as $fp) if ($fp['filter'] == $fu['filter']) {
					$filters[] = $fp['uri'];
				}
			}

			if (count($filters)) {
				$restApi->assignFiltersToUser($filters, $user['uid'], $params['pid']);
			}

		}

		$this->logEvent('syncFilters', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $restApi->getLogPath());
		return array(
			'gdWriteStartTime'  => $gdWriteStartTime
		);
	}
}
