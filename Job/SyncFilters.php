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

class SyncFilters extends AbstractJob {

	function run($job, $params)
	{
		$gdWriteStartTime = date('c');

		try {
			$bucketAttributes = $this->configuration->bucketAttributes();
			$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

			$projects = $this->configuration->getProjects();
			if (!empty($params['pid'])) {
				$projects = array($this->configuration->getProject($params['pid']));
			}

			foreach($projects as $p) {

				$gdFilters = $this->restApi->getFilters($p['pid']);

				// Optional cleanup of filters from project
				if (isset($params['clean']) && $params['clean'] == 1) {
					foreach ($gdFilters as $gdf) {
						$this->restApi->deleteFilter($gdf['link']);
					}
				} else {
					// Download filters from GD project to SAPI config
					foreach ($gdFilters as $gdf) {
						$gdFilter = $this->restApi->get($gdf['link']);

						$filter = $this->saveFilterFromObject($gdFilter);
						if (!$this->isFilterInProject($filter['name'], $p['pid'])) {
							$this->configuration->saveFiltersProjects($filter['name'], $p['pid']);
						}

						// get users using this filter
						$userFilters = $this->restApi->get('/gdc/md/' . $p['pid'] . '/userfilters?userFilters=' . $filter['uri']);

						foreach ($userFilters['userFilters']['items'] as $uf) {
							$uid = preg_replace('/.*\//', '', $uf['user']);
							$user = $this->configuration->getUserByUid($uid);
							if ($user) {
								$this->configuration->saveFilterUser($uf['userFilters'], $user['email']);
							}
						}

					}
				}

				// get users in project
				foreach($this->configuration->getProjectUsers() as $pu) {

					if ($pu['pid'] == $p['pid']) {
						$user = $this->configuration->getUser($pu['email']);

						// get filters for user
						$filters = array();
						foreach($this->configuration->getFiltersUsers() as $fu) {
							if ($fu['userEmail'] == $user['email'] && $this->isFilterInProject($fu['filterName'], $p['pid'])) {

								// Create filter if not exists
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
				'status'            => 'success',
				'gdWriteStartTime'  => $gdWriteStartTime
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

	protected function isFilterInProject($filterName, $pid)
	{
		foreach ($this->configuration->getFiltersProjects() as $fp) {
			if ($fp['filterName'] == $filterName && $fp['pid'] == $pid) {
				return true;
			}
		}
		return false;
	}

	protected function saveFilterFromObject($obj)
	{
		$name = $obj['userFilter']['meta']['title'];

		$filterUri = $obj['userFilter']['meta']['uri'];

		$exp = $obj['userFilter']['content']['expression'];

		$opStartPos = strpos($exp, ']');
		$opEndPos = strpos($exp, '[', $opStartPos);
		$attrId = substr($exp, 1, $opStartPos-1);

		$operator = substr($exp, $opStartPos+1, $opEndPos-$opStartPos-1);
		if (strpos($operator, '(') !== false) {
			$operator = substr($operator, 0, -1);
			$opEndPos--;
		}

		$elementStr = substr($exp, $opEndPos);

		$elementArr = array();
		if (strpos($elementStr, '(') === 0) {
			$elementArr = explode(',', substr($elementStr, 1, -1));
		} else {
			$elementArr[] = $elementStr;
		}

		// Get Attribute names
		$attrObj = $this->restApi->get($attrId);
		$attrIdentifierArr = explode('.', $attrObj['attribute']['meta']['identifier']);
		$gdTableName = $attrIdentifierArr[1];
		$gdFieldName = $attrIdentifierArr[2];

		$sapiTable = $this->findTable($gdTableName);

		$attribute = $sapiTable['id'] . '.' . $gdFieldName;

		// Get Element value names
		$elementNames = array();
		foreach ($elementArr as $e) {
			$elemUri = substr($e, 1, -1);

			$elementNames[] = $this->getElementTitle($attrObj, $elemUri);
		}
		$element = join(',', $elementNames);

		if (!$this->configuration->getFilterByUri($filterUri)) {
			// Filter of the same name exists, but has different URI -> modify name of the filter
			if (false != $this->configuration->getFilter($name)) {
				$cnt = 1;
				while (false != $this->configuration->getFilter($name . '-' . $cnt)) {
					$cnt++;
				}
				$name = $name . '-' . $cnt;
			}

//			echo "Saving filter " . $name . " uri " . $filterUri . PHP_EOL ;


			$this->configuration->saveFilter($name, $attribute, $element, $operator, $filterUri);
		}

		return array(
			'name'  => $name,
			'attribute' => $attribute,
			'element'   => $element,
			'operator'  => $operator,
			'uri'       => $filterUri
		);
	}

	protected function findTable($name)
	{
		$sapiTables = $this->configuration->getTables();

		foreach ($sapiTables as $table) {

			$stripedName = str_replace(array('.','-'), '', $table['id']);

			if ($stripedName == $name) {
				return $table;
			}
		}

		return null;
	}

	protected function getElementTitle($attributeObject, $elementUri)
	{
		$attrElementsUri = $attributeObject['attribute']['content']['displayForms'][0]['links']['elements'];
		$elements = $this->restApi->getElements($attrElementsUri);

		foreach ($elements as $e) {
			if ($e['uri'] == $elementUri) {
				return $e['title'];
			}
		}

		return null;
	}

	protected function isFilterInGD($filter)
	{
		if (empty($filter['uri'])) {
			return false;
		}

		$filterObj = $this->restApi->get($filter['uri']);
	}


}
