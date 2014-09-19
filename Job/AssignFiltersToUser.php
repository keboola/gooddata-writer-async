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

	public function prepare($params)
	{
		//@TODO backwards compatibility, REMOVE SOON
		if (isset($params['userEmail'])) {
			$params['email'] = $params['userEmail'];
			unset($params['userEmail']);
		}
		////

		$this->checkParams($params, array('writerId', 'email'));
		if (!isset($params['filters'])) {
			throw new WrongParametersException($this->translator->trans('parameters.filters.required'));
		}
		$configuredFilters = array();
		foreach ($this->configuration->getFilters() as $f) {
			$configuredFilters[] = $f['name'];
		}
		foreach ($params['filters'] as $f) {
			if (!in_array($f, $configuredFilters)) {
				$filters = is_array($f)? implode(', ', $f) : $f;
				throw new WrongParametersException($this->translator->trans('parameters.filters.not_exist %1', array('%1' => $filters)));
			}
		}
		$this->checkWriterExistence($params['writerId']);

		return array(
			'filters' => $params['filters'],
			'email' => $params['email']
		);
	}

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

		foreach ($pidUris as $pid => $uris) {
			$restApi->assignFiltersToUser($uris, $user['uid'], $pid);
		}
		$this->configuration->saveFiltersToUser($params['filters'], $params['email']);

		return array();
	}
}
