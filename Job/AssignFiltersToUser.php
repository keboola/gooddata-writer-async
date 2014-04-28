<?php
/**
 * @author Miroslav Cillik <miro@keboola.com>
 * @date 2013-04-24
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\Exception\WrongParametersException;

class assignFiltersToUser extends AbstractJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongParametersException
	 * @return array
	 */
	public function run($job, $params)
	{
		$this->checkParams($params, array('userEmail', 'pid'));

		if (!is_array($params['filters'])) {
			throw new WrongParametersException("Parameter 'filters' must be an array.");
		}

		$project = $this->configuration->getProject($params['pid']);
		if ($project == false) {
			throw new WrongParametersException("Project with specified PID not found within this writer.");
		}

		$user = $this->configuration->getUser($params['userEmail']);
		if ($user == false) {
			throw new WrongParametersException("User '". $user ."' not found within this writer.");
		}

		$filterUris = array();
		foreach ($params['filters'] as $name) {
			$filter = $this->configuration->getFilter($name);
			if (!$filter['uri']) {
				throw new WrongConfigurationException("Filter '" . $name . "' does not have configured uri.");
			}
			$filterUris[] = $filter['uri'];
		}

		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		$gdWriteStartTime = date('c');
		$this->restApi->assignFiltersToUser($filterUris, $user['uid'], $params['pid']);

		$this->configuration->saveFilterUser($filterUris, $params['userEmail']);

		$this->logEvent('assignFilterToUser', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $this->restApi->getLogPath());
		return array(
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}
