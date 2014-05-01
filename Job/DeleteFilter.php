<?php
/**
 * DeleteFilter.php
 *
 * @author: Miroslav Čillík <miro@keboola.com>
 * @created: 30.4.13
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\GoodData\RestApiException;

class DeleteFilter extends AbstractJob
{
	/**
	 * required: uri
	 * optional:
	 */
	function run($job, $params)
	{
		$this->checkParams($params, array('uri'));

		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$gdWriteStartTime = date('c');
		try {
			$this->restApi->deleteFilter($params['uri']);
		} catch (RestApiException $e) {
			$mes = json_decode($e->getMessage(), true);

			if (!isset($mes['error']['errorClass']) || $mes['error']['errorClass'] != 'GDC::Exception::NotFound') {
				throw $e;
			}
		}

		$this->configuration->deleteFilter($params['uri']);

		$this->logEvent('deleteFilter', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $this->restApi->getLogPath());
		return array(
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}
