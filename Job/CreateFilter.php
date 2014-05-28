<?php
/**
 * @author Miroslav Cillik <miro@keboola.com>
 * @date 2013-04-22
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;

class createFilter extends AbstractJob
{
	/**
	 * required: name, attribute, element, pid, operator
	 * optional:
	 */
	public function run($job, $params)
	{
		$this->checkParams($params, array('name', 'attribute', 'element', 'pid', 'operator'));

		$attr = explode('.', $params['attribute']);
		if (count($attr) != 4) {
			throw new WrongConfigurationException($this->translator->trans('parameters.attribute.format'));
		}
		$tableId = sprintf('%s.%s.%s', $attr[0], $attr[1], $attr[2]);
		$sapiTable = $this->configuration->getSapiTable($tableId);
		if (!in_array($attr[3], $sapiTable['columns'])) {
			throw new WrongConfigurationException($this->translator->trans('parameters.attribute.not_found'));
		}

		$attrId = $this->configuration->translateAttributeName($params['attribute']);

		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		$gdWriteStartTime = date('c');
		$filterUri = $this->restApi->createFilter(
			$params['name'],
			$attrId,
			$params['element'],
			$params['operator'],
			$params['pid']
		);

		$this->configuration->saveFilter(
			$params['name'],
			$params['attribute'],
			$params['element'],
			$params['operator'],
			$filterUri
		);

		$this->configuration->saveFiltersProjects($params['name'], $params['pid']);

		$this->logEvent('createFilter', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $this->restApi->getLogPath());
		return array(
			'uri' => $filterUri,
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}
