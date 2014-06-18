<?php
/**
 *
 */

namespace Keboola\GoodDataWriter\Service\Aws;

use Guzzle\Log\AbstractLogAdapter,
	Monolog\Logger;

class BackoffLogAdapter extends AbstractLogAdapter
{
	private $logger;

	/**
	 * @param Logger $logger
	 */
	public function __construct(Logger $logger)
	{
		$this->logger = $logger;
	}

	/**
	 * Log a message at a priority
	 *
	 * @param string $message  Message to log
	 * @param integer $priority Priority of message (use the \LOG_* constants of 0 - 7)
	 * @param array $extras   Extra information to log in event
	 */
	public function log($message, $priority = LOG_INFO, $extras = array())
	{
		$this->logger->log('backoff', $priority, array(
			'backoff' => $message,
		));
	}

}