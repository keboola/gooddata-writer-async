<?php
/**
 * @author Miroslav Cillik <miro@keboola.com>
 * @date 2013-04-22
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\GoodData\RestApi;

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

		$attr = explode('.', $params['attribute']);
		if (count($attr) != 4) {
			throw new WrongParametersException($this->translator->trans('parameters.attribute.format'));
		}
		$tableId = sprintf('%s.%s.%s', $attr[0], $attr[1], $attr[2]);
		$sapiTable = $this->configuration->getSapiTable($tableId);
		if (!in_array($attr[3], $sapiTable['columns'])) {
			throw new WrongParametersException($this->translator->trans('parameters.attribute.not_found'));
		}

		return array(
			'name' => $params['name'],
			'attribute' => $params['attribute'],
			'value' => $params['value'],
			'pid' => $params['pid'],
			'operator' => $params['operator']
		);
	}

	/**
	 * required: name, attribute, element, pid, operator
	 * optional:
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

		$attr = explode('.', $params['attribute']);
		if (count($attr) != 4) {
			throw new WrongParametersException($this->translator->trans('parameters.attribute.format'));
		}
		$tableId = sprintf('%s.%s.%s', $attr[0], $attr[1], $attr[2]);
		$sapiTable = $this->configuration->getSapiTable($tableId);
		if (!in_array($attr[3], $sapiTable['columns'])) {
			throw new WrongParametersException($this->translator->trans('parameters.attribute.not_found'));
		}

		$attrId = $this->configuration->translateAttributeName($params['attribute']);

		$bucketAttributes = $this->configuration->bucketAttributes();
		$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		$gdWriteStartTime = date('c');
		$filterUri = $restApi->createFilter($params['name'], $attrId, $params['operator'], $params['value'], $params['pid']);

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
