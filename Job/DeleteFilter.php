<?php
/**
 * DeleteFilter.php
 *
 * @author: Miroslav Čillík <miro@keboola.com>
 * @created: 30.4.13
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\GoodData\RestApiException;

class DeleteFilter extends AbstractJob
{
	/**
	 * required: uri|name
	 * optional:
	 */
	function run($job, $params, RestApi $restApi)
	{
		$uris = array();
		if (isset($params['name'])) {
			// Delete filter in all projects
			foreach ($this->configuration->getFiltersProjectsByFilter($params['name']) as $fp) {
				$uris[] = $fp['uri'];
			}
		} else {
			// Delete filter only from particular project
			$this->checkParams($params, array('uri'));
			if (!$this->configuration->checkFilterUri($params['uri'])) {
				throw new WrongParametersException($this->translator->trans('parameters.filters.not_exist %1', array('%1' => $params['uri'])));
			}
			$uris[] = $params['uri'];
		}

		$bucketAttributes = $this->configuration->bucketAttributes();
		$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$gdWriteStartTime = date('c');

		foreach ($uris as $uri) {
			try {
				$restApi->deleteFilter($uri);
			} catch (RestApiException $e) {
				$message = json_decode($e->getMessage(), true);
				if (!isset($message['error']['errorClass']) || $message['error']['errorClass'] != 'GDC::Exception::NotFound') {
					throw $e;
				}
			}
		}

		if (isset($params['name'])) {
			$this->configuration->deleteFilter($params['name']);
		} else {
			$this->configuration->deleteFilterFromProject($params['uri']);
		}

		$this->logEvent('deleteFilter', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $restApi->getLogPath());
		return array(
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}
