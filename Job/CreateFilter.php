<?php
/**
 * @author Miroslav Cillik <miro@keboola.com>
 * @date 2013-04-22
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\Writer\Configuration;

class createFilter extends AbstractJob
{

	public function prepare($params)
	{
		//@TODO backwards compatibility, REMOVE SOON
		if (isset($params['element'])) {
			$params['value'] = $params['element'];
			unset($params['element']);
		}
		$this->checkParams($params, array('writerId', 'name', 'attribute', 'value', 'pid'));
		$this->checkWriterExistence($params['writerId']);
		if (!isset($params['operator'])) {
			$params['operator'] = '=';
		}

		if ($this->configuration->getFilter($params['name'])) {
			throw new WrongParametersException($this->translator->trans('parameters.filter.already_exists'));
		}

		$result = array(
			'name' => $params['name'],
			'attribute' => $params['attribute'],
			'value' => $params['value'],
			'pid' => $params['pid'],
			'operator' => $params['operator']
		);

		$this->configuration->getTableIdFromAttribute($params['attribute']);
		if ((isset($params['over']) && !isset($params['to'])) || (!isset($params['over']) && isset($params['to']))) {
			throw new WrongParametersException($this->translator->trans('parameters.filter.over_to_missing'));
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
	public function run($job, $params, RestApi $restApi)
	{
		$this->checkParams($params, array('name', 'attribute', 'operator', 'value', 'pid'));

		$filter = $this->configuration->getFilter($params['name']);
		if ($filter) {
			foreach ($this->configuration->getFiltersProjectsByFilter($params['name']) as $fp) {
				if ($fp == $params['pid']) {
					throw new WrongParametersException($this->translator->trans('parameters.filter.already_exists'));
				}
			}
		}

		$tableId = $this->configuration->getTableIdFromAttribute($params['attribute']);
		$tableDefinition = $this->configuration->getDataSet($tableId);
		$tableName = empty($tableDefinition['name'])? $tableId : $tableDefinition['name'];
		$attrName = substr($params['attribute'], strrpos($params['attribute'], '.') + 1);
		$attrId = Model::getAttributeId($tableName, $attrName);

		$overAttrId = null;
		$toAttrId = null;
		if ((isset($params['over']) && !isset($params['to'])) || (!isset($params['over']) && isset($params['to']))) {
			throw new WrongParametersException($this->translator->trans('parameters.filter.over_to_missing'));
		}
		if (isset($params['over']) && isset($params['to'])) {
			$overTableId = $this->configuration->getTableIdFromAttribute($params['over']);
			$overTableDefinition = $this->configuration->getDataSet($overTableId);
			$overTableName = empty($overTableDefinition['name'])? $overTableId : $overTableDefinition['name'];
			$overAttrName = substr($params['over'], strrpos($params['over'], '.') + 1);
			$overAttrId = Model::getAttributeId($overTableName, $overAttrName);

			$toTableId = $this->configuration->getTableIdFromAttribute($params['to']);
			$toTableDefinition = $this->configuration->getDataSet($toTableId);
			$toTableName = empty($toTableDefinition['name'])? $toTableId : $toTableDefinition['name'];
			$toAttrName = substr($params['to'], strrpos($params['to'], '.') + 1);
			$toAttrId = Model::getAttributeId($toTableName, $toAttrName);
		}

		$bucketAttributes = $this->configuration->bucketAttributes();
		$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		$gdWriteStartTime = date('c');
		$filterUri = $restApi->createFilter($params['name'], $attrId, $params['operator'], $params['value'], $params['pid'], $overAttrId, $toAttrId);

		$this->configuration->saveFilter($params['name'], $params['attribute'], $params['operator'], $params['value']);
		$this->configuration->saveFiltersProjects($filterUri, $params['name'], $params['pid']);

		$this->logEvent('createFilter', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $restApi->getLogPath());
		return array(
			'uri' => $filterUri,
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}
