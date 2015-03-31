<?php
/**
 * @author Miroslav Cillik <miro@keboola.com>
 * @date 2013-04-22
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\Job\Metadata\Job;
use Keboola\Syrup\Exception\UserException;

class CreateFilter extends AbstractTask
{

    public function prepare($params)
    {
        //@TODO backwards compatibility, REMOVE SOON
        if (isset($params['element'])) {
            $params['value'] = $params['element'];
            unset($params['element']);
        }
        $this->checkParams($params, ['writerId', 'name', 'attribute', 'value', 'pid']);
        $this->checkWriterExistence($params['writerId']);
        if (!isset($params['operator'])) {
            $params['operator'] = '=';
        }

        if ($this->configuration->getFilter($params['name'])) {
            throw new UserException($this->translator->trans('parameters.filters.already_exists'));
        }

        $result = [
            'name' => $params['name'],
            'attribute' => $params['attribute'],
            'value' => $params['value'],
            'pid' => $params['pid'],
            'operator' => $params['operator']
        ];

        $this->configuration->getTableIdFromAttribute($params['attribute']);
        if ((isset($params['over']) && !isset($params['to'])) || (!isset($params['over']) && isset($params['to']))) {
            throw new UserException($this->translator->trans('parameters.filters.over_to_missing'));
        }
        if (isset($params['over']) && isset($params['to'])) {
            $this->configuration->getTableIdFromAttribute($params['over']);
            $this->configuration->getTableIdFromAttribute($params['to']);
            $result['over'] = $params['over'];
            $result['to'] = $params['to'];
        }

        return $result;
    }

    /**
     * required: name, attribute, operator, value ,pid
     * optional: over, to
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);
        $this->checkParams($params, ['name', 'attribute', 'operator', 'value', 'pid']);

        $filter = $this->configuration->getFilter($params['name']);
        if ($filter) {
            foreach ($this->configuration->getFiltersProjectsByFilter($params['name']) as $fp) {
                if ($fp == $params['pid']) {
                    throw new UserException($this->translator->trans('parameters.filters.already_exists'));
                }
            }
        }

        $tableId = $this->configuration->getTableIdFromAttribute($params['attribute']);
        $tableDefinition = $this->configuration->getDataSet($tableId);
        $attrName = substr($params['attribute'], strrpos($params['attribute'], '.') + 1);
        $attrId = Model::getAttributeId($tableDefinition['title'], $attrName);

        $overAttrId = null;
        $toAttrId = null;
        if ((isset($params['over']) && !isset($params['to'])) || (!isset($params['over']) && isset($params['to']))) {
            throw new UserException($this->translator->trans('parameters.filters.over_to_missing'));
        }
        if (isset($params['over']) && isset($params['to'])) {
            $overTableId = $this->configuration->getTableIdFromAttribute($params['over']);
            $overTableDefinition = $this->configuration->getDataSet($overTableId);
            $overAttrName = substr($params['over'], strrpos($params['over'], '.') + 1);
            $overAttrId = Model::getAttributeId($overTableDefinition['title'], $overAttrName);

            $toTableId = $this->configuration->getTableIdFromAttribute($params['to']);
            $toTableDefinition = $this->configuration->getDataSet($toTableId);
            $toAttrName = substr($params['to'], strrpos($params['to'], '.') + 1);
            $toAttrId = Model::getAttributeId($toTableDefinition['title'], $toAttrName);
        }

        $bucketAttributes = $this->configuration->getBucketAttributes();
        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

        $filterUri = $this->restApi->createFilter($params['name'], $attrId, $params['operator'], $params['value'], $params['pid'], $overAttrId, $toAttrId);

        $this->configuration->saveFilter(
            $params['name'],
            $params['attribute'],
            $params['operator'],
            $params['value'],
            isset($params['over']) ? $params['over'] : null,
            isset($params['to']) ? $params['to'] : null
        );
        $this->configuration->saveFiltersProjects($filterUri, $params['name'], $params['pid']);

        return [
            'uri' => $filterUri
        ];
    }
}
