<?php
/**
 * @author Ondrej Hlavacek <ondrej.hlavacek@keboola.com>
 * @date 2014-03-06
 */

namespace Keboola\GoodDataWriter\Model;

use Keboola\GoodDataWriter\Exception\GraphTtlException;
use Keboola\GoodDataWriter\Writer\Configuration;

class Graph
{
	/**
	 * @var string
	 */
	private $dimensionsUrl = '';

	/**
	 * @var string
	 */
	private $tableUrl = '';

	/**
	 * @param $url
	 * @return $this
	 */
	public function setDimensionsUrl($url)
	{
		$this->dimensionsUrl = $url;
		return $this;
	}

	/**
	 * @return string
	 */
	public function getDimensionsUrl()
	{
		return $this->dimensionsUrl;
	}

	/**
	 * @param $url
	 */
	public function setTableUrl($url)
	{
		$this->tableUrl = $url;
		return $this;
	}

	/**
	 * @return string
	 */
	public function getTableUrl()
	{
		return $this->tableUrl;
	}


	/**
	 *
	 * generate model
	 *
	 * @return array
	 */
	public function getGraph(Configuration $configuration)
	{
		$data = array();
		// Merge tables and dimensions
		$data["nodes"] = array_merge($this->getTables($configuration), $this->getDimensions($configuration));
		// Get transitions
		$transitions = $this->getTransitions($configuration);
		// Compute transitivity
		$transitivePaths = $this->getTransitivePaths($transitions);
		// Apply transitivity flag to paths
		foreach($transitions as $key => $transition) {
			foreach($transitivePaths as $transitivePath) {
				if ($transition["source"] == $transitivePath["source"]
					&& $transition["target"] == $transitivePath["target"]) {
					$transitions[$key]['transitive'] = true;
				}
			}
		}
		$data["transitions"] = $transitions;
		return $data;
	}

	/**
	 *
	 * return all tables
	 *
	 * @@param Configuration $configuration
	 * @return array
	 */
	public function getTables(Configuration $configuration)
	{
		$tables = array();
		foreach ($configuration->getDataSets() as $dataSet) {
			if (!empty($dataSet['export'])) {
				$tables[] = array(
					'node' => $dataSet['id'],
					'label' => !empty($dataSet['name']) ? $dataSet['name'] : $dataSet['id'],
					'type' => 'dataset',
					'link' => $this->getTableUrl() . $dataSet['id']
				);
			}
		}
		return $tables;
	}

	/**
	 *
	 * return all dimensions
	 *
	 * @param Configuration $configuration
	 * @return array
	 */
	public function getDimensions(Configuration $configuration)
	{
		$dimensions = array();
		$dimensionIds = array();
		foreach ($configuration->getDataSets() as $dataSet) {
			if (!empty($dataSet['export'])) {
				$definition = $configuration->getDataSet($dataSet['id']);
				foreach ($definition['columns'] as $c) {
					if ($c['type'] == 'DATE' && $c['dateDimension']) {
						if (!in_array($c['dateDimension'], $dimensionIds)) {
							$dimensions[] = array(
								'node' => 'dim.' . $c['dateDimension'],
								'label' => $c['dateDimension'],
								'type' => 'dimension',
								'link' => $this->getDimensionsUrl()
							);
							$dimensionIds[] = $c['dateDimension'];
						}
					}
				}
			}
		}
		return $dimensions;
	}

	/**
	 *
	 * return all transitions
	 *
	 * @@param Configuration $configuration
	 * @return array
	 */
	public function getTransitions(Configuration $configuration)
	{
		$transitions = array();
		foreach ($configuration->getDataSets() as $dataSet) {
			if (!empty($dataSet['export'])) {
				$definition = $configuration->getDataSet($dataSet['id']);
				foreach ($definition['columns'] as $c) {
					if ($c['type'] == 'DATE' && $c['dateDimension']) {
						$transitions[] = array(
							'source' => $dataSet['id'],
							'target' => 'dim.' . $c['dateDimension'],
							'type' => 'dimension',
							'transitive' => false
						);
					}
					if ($c['type'] == 'REFERENCE' && $c['schemaReference']) {
						$transitions[] = array(
							'source' => $dataSet['id'],
							'target' => $c['schemaReference'],
							'type' => 'dataset',
							'transitive' => false
						);
					}
				}
			}
		}
		return $transitions;
	}

	/**
	 *
	 * get paths that are transitive
	 *
	 * @param array $transitions
	 * @return array
	 */
	public function getTransitivePaths($transitions)
	{
		$endPoints = $this->getEndPoints($transitions);

		// Get all routes
		$allTransitivePaths = array();
		foreach($endPoints as $endPoint) {
			$paths = [];
			$this->findRoutes($endPoint, $transitions, $paths);

			// Remove partials
			foreach($paths as $key => $path) {
				if ($path["partial"]) {
					unset($paths[$key]);
				}
			}

			// Merge transitive paths
			foreach ($paths as $key => $path) {
				foreach ($paths as $key2 => $path2) {
					if ($path["source"] == $path2["source"] && $path["target"] == $path2["target"] && $key != $key2) {
						// Get shared paths
						$transitionIds = array();
						$transitionIds2 = array();
						foreach($path["path"] as $transition) {
							$transitionIds[] = $transition['source'] . '|' . $transition['target'];
						}
						foreach($path2["path"] as $transition) {
							$transitionIds2[] = $transition['source'] . '|' . $transition['target'];
						}
						$commonTransitions = array_intersect($transitionIds, $transitionIds2);

						// Add only paths, that are not shared in the two routes
						foreach($path["path"] as $transition) {
							$transitionId = $transition['source'] . '|' . $transition['target'];
							if (!in_array($transitionId, $commonTransitions)) {
								$allTransitivePaths[] = $transition;
							}
						}
						foreach($path2["path"] as $transition) {
							$transitionId = $transition['source'] . '|' . $transition['target'];
							if (!in_array($transitionId, $commonTransitions)) {
								$allTransitivePaths[] = $transition;
							}
						}
					}
				}
			}
		}

		// Dedup
		$uniqueTransitivePaths = array();
		$uniqueTransitivePathIds = array();
		foreach ($allTransitivePaths as $transitivePath) {
			$transitionId = $transitivePath['source'] . '|' . $transitivePath['target'];
			if (!in_array($transitionId, $uniqueTransitivePathIds)) {
				$uniqueTransitivePathIds[] = $transitionId;
				$uniqueTransitivePaths[] = $transitivePath;

			}
		}
		return $uniqueTransitivePaths;
	}


	/**
	 *
	 * Find all paths that lead to given node
	 *
	 * @param $node
	 * @param $transitions
	 * @param $startPoints
	 * @param $endPoints
	 * @param $paths
	 * @param int $ttl
	 * @throws GraphTtlException
	 */
	public function findRoutes($node, $transitions, &$paths, $ttl=20) {
		if ($ttl <= 0) {
			throw new GraphTtlException("TTL exceeded. Model not computable.");
		}
		foreach($transitions as $transition) {
			if ($transition["target"] == $node) {
				$this->addTransition($paths, $transition["source"], $transition["target"]);
			}
		}
		$startPoints = $this->getStartPoints($transitions);
		$endPoints = $this->getEndPoints($transitions);
		foreach($paths as $key => $path) {
			// Final path goes from a start point to an end point
			if (in_array($path["source"], $startPoints) && in_array($path["target"], $endPoints) && !$paths[$key]["final"]) {
				$paths[$key]["final"] = true;
			}
			// Every path has to go from a start point to an end point
			// Go one step further for non-finalized and non-partial paths
			if (!$paths[$key]["final"] && !$paths[$key]["partial"]) {
				$this->findRoutes($paths[$key]["source"], $transitions, $paths, $ttl-1);
			}
		}
	}

	/**
	 *
	 *
	 * Add a new transition to paths
	 * If it is appending to existing paths, the path is duplicated and marked as partial
	 * and a new one with new the new transition is created - this allows for multiple paths
	 * from a single node
	 *
	 * @param $paths
	 * @param $source
	 * @param $target
	 */
	public function addTransition(&$paths, $source, $target) {
		// Append
		$found = false;
		foreach ($paths as $key => $path) {
			if ($path["source"] == $target) {
				$paths[$key]["partial"] = true;
				$pathPieces = $path["path"];
				$pathPieces[] = array(
					"source" => $source,
					"target" => $target
				);
				$paths[] = array(
					"source" => $source,
					"target" => $path["target"],
					"final" => false,
					"partial" => false,
					"path" => $pathPieces
				);
				$found = true;
			}
		}

		// Create if not appended
		if (!$found) {
			$paths[] = array(
				"source" => $source,
				"target" => $target,
				"final" => false,
				"partial" => false,
				"path" => array(
					array(
						"source" => $source,
						"target" => $target
					)
				)
			);
		}
	}


	/**
	 *
	 * return start points in a graph
	 *
	 * @param $transitions
	 * @return array
	 */
	public function getStartPoints($transitions)
	{
		$startPoints = [];
		foreach($transitions as $transition) {
			$startPoints[] = $transition["source"];
		}
		$startPoints = array_unique($startPoints);
		foreach($transitions as $transition) {
			if (in_array($transition["target"], $startPoints)) {
				unset($startPoints[array_search($transition["target"], $startPoints)]);
			}
		}
		sort($startPoints);
		return $startPoints;
	}

	/**
	 *
	 * return endpoints in a graph
	 *
	 * @param $transitions
	 * @return array
	 */
	public function getEndPoints($transitions)
	{
		$endPoints = [];
		foreach($transitions as $transition) {
			$endPoints[] = $transition["target"];
		}
		$endPoints = array_unique($endPoints);
		foreach($transitions as $transition) {
			if (in_array($transition["source"], $endPoints)) {
				unset($endPoints[array_search($transition["source"], $endPoints)]);
			}
		}
		sort($endPoints);
		return $endPoints;
	}
}
