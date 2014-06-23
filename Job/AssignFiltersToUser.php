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
	 * required: email, pid, filters
	 * optional:
	 */
	public function run($job, $params)
	{
		$this->checkParams($params, array('email', 'pid'));
		$params['email'] = strtolower($params['email']);

		if (!is_array($params['filters'])) {
			throw new WrongParametersException($this->translator->trans('configuration.filters.not_array'));
		}

		$project = $this->configuration->getProject($params['pid']);
		if ($project == false) {
			throw new WrongParametersException($this->translator->trans('parameters.pid_not_configured'));
		}

		$user = $this->configuration->getUser($params['email']);
		if ($user == false) {
			throw new WrongParametersException($this->translator->trans('parameters.email_not_configured'));
		}

		$configuredFilters = array();
		foreach ($this->configuration->getFilters() as $f) {
			$configuredFilters[] = $f['name'];
		}

		$uris = array();
		foreach ($params['filters'] as $name) {
			if (!in_array($name, $configuredFilters)) {
				throw new WrongParametersException($this->translator->trans('parameters.filters.not_exist %1', array('%1' => $name)));
			}
			foreach ($this->configuration->getFilterInProjects($name) as $fp) {
				if (!$fp['uri']) {
					throw new WrongConfigurationException($this->translator->trans('configuration.filter.missing_uri %1', array('%1' => $name)));
				}
				$uris[] = $fp['uri'];
			}
		}

		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		$gdWriteStartTime = date('c');
		$this->restApi->assignFiltersToUser($uris, $user['uid'], $params['pid']);

		$this->configuration->saveFiltersToUser($params['filters'], $params['email']);

		$this->logEvent('assignFilterToUser', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $this->restApi->getLogPath());
		return array(
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}
