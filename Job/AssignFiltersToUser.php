<?php
/**
 * @author Miroslav Cillik <miro@keboola.com>
 * @date 2013-04-24
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;

class assignFiltersToUser extends AbstractJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		$this->checkParams($params, array('filters', 'userEmail', 'pid'));

		if (!is_array($params['filters'])) {
			throw new WrongConfigurationException("Parameter 'filters' must be an array.");
		}

		$user = $this->configuration->getUser($params['userEmail']);
		$filterUris = array();
		foreach ($params['filters'] as $name) {
			$filter = $this->configuration->getFilter($name);
			$filterUris[] = $filter['uri'];
		}

		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		$gdWriteStartTime = date('c');
		$this->restApi->assignFiltersToUser($filterUris, $user['uid'], $params['pid']);

		$this->configuration->saveFilterUser($filterUris, $params['userEmail']);

		return array(
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}
