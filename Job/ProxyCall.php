<?php
/**
 * Proxy Call Job
 *
 * User: mirocillik
 * Date: 19/11/13
 * Time: 15:00
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\GoodData\RestApi;

class ProxyCall  extends AbstractJob
{
	/**
	 * required: query, payload
	 * optional: pid
	 */
	function run($job, $params, RestApi $restApi)
	{
		$this->checkParams($params, array('query', 'payload'));

		$gdWriteStartTime = date('c');

		$bucketAttributes = $this->configuration->bucketAttributes();
		$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$response = $restApi->post($params['query'], $params['payload']);

		$this->logEvent('proxyCall', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $restApi->getLogPath());
		return array(
			'response' => $response,
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}