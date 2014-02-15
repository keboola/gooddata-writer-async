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
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		$this->checkParams($params, array('name', 'attribute', 'element', 'pid', 'operator'));

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
