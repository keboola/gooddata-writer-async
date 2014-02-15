<?php
/**
 * Proxy Call Job
 *
 * User: mirocillik
 * Date: 19/11/13
 * Time: 15:00
 */

namespace Keboola\GoodDataWriter\Job;

class ProxyCall  extends AbstractJob
{
	function run($job, $params)
	{
		$this->checkParams($params, array('query', 'payload'));

		$gdWriteStartTime = date('c');

		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$response = $this->restApi->post($params['query'], $params['payload']);

		$this->logEvent('proxyCall', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $this->restApi->getLogPath());
		return array(
			'response' => $response,
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}