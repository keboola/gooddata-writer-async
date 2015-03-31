<?php
/**
 * SyncFilters.php
 *
 * @author: Miroslav Čillík <miro@keboola.com>
 * @created: 3.5.13
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\Job\Metadata\Job;
use Keboola\GoodDataWriter\Writer\Configuration;

class SyncFilters extends AbstractTask
{

    public function prepare($params)
    {
        $this->checkParams($params, ['writerId']);
        $this->checkWriterExistence($params['writerId']);

        return [
            'pid' => empty($params['pid'])? null : $params['pid']
        ];
    }

    /**
     * required:
     * optional: pid
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);
        $this->configuration->checkTable(Configuration::FILTERS_TABLE_NAME);
        $bucketAttributes = $this->configuration->getBucketAttributes();
        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

        // Delete all filters from project
        $gdFilters = $this->restApi->getFilters($params['pid']);
        foreach ($gdFilters as $gdf) {
            $this->restApi->deleteFilter($gdf['link']);
        }

        // Create filters
        $filterUris = [];
        $filtersToCreate = [];
        foreach ($this->configuration->getFiltersProjectsByPid($params['pid']) as $fp) {
            $filtersToCreate[] = $fp['filter'];
            $this->configuration->deleteFilterFromProject($fp['uri']);
        }
        foreach ($this->configuration->getFilters() as $f) {
            if (in_array($f['name'], $filtersToCreate)) {
                $tableId = $this->configuration->getTableIdFromAttribute($f['attribute']);
                $tableDefinition = $this->configuration->getDataSet($tableId);
                $attrName = substr($f['attribute'], strrpos($f['attribute'], '.') + 1);
                $attrId = Model::getAttributeId($tableDefinition['title'], $attrName);

                $overAttrId = $toAttrId = null;
                if (!empty($f['over']) && !empty($f['to'])) {
                    $overTableId = $this->configuration->getTableIdFromAttribute($f['over']);
                    $overTableDefinition = $this->configuration->getDataSet($overTableId);
                    $overAttrName = substr($f['over'], strrpos($f['over'], '.') + 1);
                    $overAttrId = Model::getAttributeId($overTableDefinition['title'], $overAttrName);

                    $toTableId = $this->configuration->getTableIdFromAttribute($f['to']);
                    $toTableDefinition = $this->configuration->getDataSet($toTableId);
                    $toAttrName = substr($f['to'], strrpos($f['to'], '.') + 1);
                    $toAttrId = Model::getAttributeId($toTableDefinition['title'], $toAttrName);
                }

                $filterUris[$f['name']] = $this->restApi->createFilter(
                    $f['name'],
                    $attrId,
                    $f['operator'],
                    $f['value'],
                    $params['pid'],
                    $overAttrId,
                    $toAttrId
                );
                $this->configuration->saveFiltersProjects($filterUris[$f['name']], $f['name'], $params['pid']);
            }
        }

        // Assign filters to users
        $filtersUsers = $this->configuration->getFiltersUsers();
        $filtersProjects = $this->configuration->getFiltersProjectsByPid($params['pid']);
        foreach ($this->configuration->getProjectUsers($params['pid']) as $pu) {
            $user = $this->configuration->getUser($pu['email']);

            // get filters for user
            $filters = [];
            foreach ($filtersUsers as $fu) {
                if ($fu['email'] == $user['email']) {
                    foreach ($filtersProjects as $fp) {
                        if ($fp['filter'] == $fu['filter']) {
                            $filters[] = $fp['uri'];
                        }
                    }
                }
            }

            if (count($filters)) {
                $this->restApi->assignFiltersToUser($filters, $user['uid'], $params['pid']);
            }

        }

        return [];
    }
}
