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
		$this->configuration->checkFiltersTable();
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

			$overAttrId = $toAttrId = null;
			if (!empty($f['over']) && !empty($f['to'])) {
				$overTableId = $this->configuration->getTableIdFromAttribute($f['over']);
				$overTableDefinition = $this->configuration->getDataSet($overTableId);
				$overTableName = empty($overTableDefinition['name'])? $overTableId : $overTableDefinition['name'];
				$overAttrName = substr($f['over'], strrpos($f['over'], '.') + 1);
				$overAttrId = Model::getAttributeId($overTableName, $overAttrName);

				$toTableId = $this->configuration->getTableIdFromAttribute($f['to']);
				$toTableDefinition = $this->configuration->getDataSet($toTableId);
				$toTableName = empty($toTableDefinition['name'])? $toTableId : $toTableDefinition['name'];
				$toAttrName = substr($f['to'], strrpos($f['to'], '.') + 1);
				$toAttrId = Model::getAttributeId($toTableName, $toAttrName);
			}

			$filterUris[$f['name']] = $restApi->createFilter($f['name'], $attrId, $f['operator'], $value, $params['pid'], $overAttrId, $toAttrId);
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

		return array();
	}
}
