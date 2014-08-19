<?php
/**
 * @author Miroslav Cillik <miro@keboola.com>
 * @date 2013-04-24
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\GoodData\RestApi;

class assignFiltersToUser extends AbstractJob
{
	/**
	 * required: email, filters
	 * optional:
	 */
	public function run($job, $params, RestApi $restApi)
	{
		$this->checkParams($params, array('email'));
		$params['email'] = strtolower($params['email']);

		if (!is_array($params['filters'])) {
			throw new WrongParametersException($this->translator->trans('configuration.filters.not_array'));
		}

		$user = $this->configuration->getUser($params['email']);
		if ($user == false) {
			throw new WrongParametersException($this->translator->trans('parameters.email_not_configured'));
		}

		$configuredFilters = array();
		foreach ($this->configuration->getFilters() as $f) {
			$configuredFilters[] = $f['name'];
		}

		$pidUris = array();
		foreach ($params['filters'] as $name) {
			if (!in_array($name, $configuredFilters)) {
				throw new WrongParametersException($this->translator->trans('parameters.filters.not_exist %1', array('%1' => $name)));
			}
			foreach ($this->configuration->getFiltersProjectsByFilter($name) as $fp) {
				if (!$fp['uri']) {
					throw new WrongConfigurationException($this->translator->trans('configuration.filter.missing_uri %1', array('%1' => $name)));
				}
				$pidUris[$fp['pid']][] = $fp['uri'];
			}
		}

		$bucketAttributes = $this->configuration->bucketAttributes();
		$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		$gdWriteStartTime = date('c');

		foreach ($pidUris as $pid => $uris) {
			$restApi->assignFiltersToUser($uris, $user['uid'], $pid);
		}
		$this->configuration->saveFiltersToUser($params['filters'], $params['email']);

		$this->logEvent('assignFilterToUser', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $restApi->getLogPath());
		return array(
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}
