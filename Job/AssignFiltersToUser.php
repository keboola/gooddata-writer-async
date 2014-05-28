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
	 * required: userEmail, pid, filters
	 * optional:
	 */
	public function run($job, $params)
	{
		$this->checkParams($params, array('userEmail', 'pid'));
		$params['userEmail'] = strtolower($params['userEmail']);

		if (!is_array($params['filters'])) {
			throw new WrongParametersException($this->translator->trans('configuration.filters.not_array'));
		}

		$project = $this->configuration->getProject($params['pid']);
		if ($project == false) {
			throw new WrongParametersException($this->translator->trans('parameters.pid_not_configured'));
		}

		$user = $this->configuration->getUser($params['userEmail']);
		if ($user == false) {
			throw new WrongParametersException($this->translator->trans('parameters.email_not_configured'));
		}

		$filterUris = array();
		foreach ($params['filters'] as $name) {
			$filter = $this->configuration->getFilter($name);
			if (!$filter['uri']) {
				throw new WrongConfigurationException($this->translator->trans('configuration.filter.missing_uri %1', array('%1' => $name)));
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
