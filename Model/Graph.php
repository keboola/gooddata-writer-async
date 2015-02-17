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
        $model = $this->getTransitions($configuration);

        // Compute transitivity
        $transitions = array();
        foreach ($model as $transition) {
            $transitions[] = array(
                "tSource" => $transition["source"],
                "tTarget" => $transition["target"]
            );
        }
        $transitivePaths = $this->getTransitiveTransitions($transitions);

        // Apply transitivity flag to paths
        foreach ($model as $key => $transition) {
            foreach ($transitivePaths as $transitivePath) {
                if ($transition["source"] == $transitivePath["tSource"]
                    && $transition["target"] == $transitivePath["tTarget"]) {
                    $model[$key]['transitive'] = true;
                }
            }
        }
        $data["transitions"] = $model;
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
        $dataSets = $configuration->getDataSets();
        foreach ($dataSets as $dataSet) {
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
                        // Check, if the counterpart exists
                        $active = false;
                        foreach ($dataSets as $dataSetToCheck) {
                            if ($dataSetToCheck['id'] == $c['schemaReference'] && !empty($dataSetToCheck['export'])) {
                                $active = true;
                            }
                        }
                        if ($active) {
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
    public function getTransitiveTransitions($transitions)
    {
        $endPoints = $this->getEndPoints($transitions);

        // Get all routes
        $allTransitivePaths = array();
        foreach ($endPoints as $endPoint) {
            $paths = [];
            $this->findRoutes($endPoint, $transitions, $paths);
            // Remove partials
            foreach ($paths as $key => $path) {
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
                        foreach ($path["path"] as $transition) {
                            $transitionIds[] = $transition['tSource'] . '|' . $transition['tTarget'];
                        }
                        foreach ($path2["path"] as $transition) {
                            $transitionIds2[] = $transition['tSource'] . '|' . $transition['tTarget'];
                        }
                        $commonTransitions = array_intersect($transitionIds, $transitionIds2);

                        // Add only paths, that are not shared in the two routes
                        foreach ($path["path"] as $transition) {
                            $transitionId = $transition['tSource'] . '|' . $transition['tTarget'];
                            if (!in_array($transitionId, $commonTransitions)) {
                                $allTransitivePaths[] = $transition;
                            }
                        }
                        foreach ($path2["path"] as $transition) {
                            $transitionId = $transition['tSource'] . '|' . $transition['tTarget'];
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
            $transitionId = $transitivePath['tSource'] . '|' . $transitivePath['tTarget'];
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
    public function findRoutes($node, $transitions, &$paths, $ttl = 20)
    {
        if ($ttl <= 0) {
            throw new GraphTtlException("TTL exceeded. Model not computable.");
        }
        foreach ($transitions as $transition) {
            if ($transition["tTarget"] == $node) {
                $this->addTransition($paths, $transition["tSource"], $transition["tTarget"]);
            }
        }
        $startPoints = $this->getStartPoints($transitions);
        $endPoints = $this->getEndPoints($transitions);
        foreach ($paths as $key => $path) {
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
    public function addTransition(&$paths, $source, $target)
    {
        // Append
        $found = false;
        foreach ($paths as $key => $path) {
            if ($path["source"] == $target) {
                $paths[$key]["partial"] = true;
                $pathPieces = $path["path"];
                $pathPieces[] = array(
                    "tSource" => $source,
                    "tTarget" => $target
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
                        "tSource" => $source,
                        "tTarget" => $target
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
        foreach ($transitions as $transition) {
            $startPoints[] = $transition["tSource"];
        }
        $startPoints = array_unique($startPoints);
        foreach ($transitions as $transition) {
            if (in_array($transition["tTarget"], $startPoints)) {
                unset($startPoints[array_search($transition["tTarget"], $startPoints)]);
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
        foreach ($transitions as $transition) {
            $endPoints[] = $transition["tTarget"];
        }
        $endPoints = array_unique($endPoints);
        foreach ($transitions as $transition) {
            if (in_array($transition["tSource"], $endPoints)) {
                unset($endPoints[array_search($transition["tSource"], $endPoints)]);
            }
        }
        sort($endPoints);
        return $endPoints;
    }
}
