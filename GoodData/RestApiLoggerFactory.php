<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 06.02.14
 * Time: 13:57
 */

namespace Keboola\GoodDataWriter\GoodData;


use Keboola\GoodDataWriter\Service\RestAPILogFormatter;
use Monolog\Handler\SyslogHandler;
use Monolog\Logger;

class RestApiLoggerFactory
{
	private $logger;

	public function __construct(Logger $logger, SyslogHandler $sysLogHandler, RestAPILogFormatter $logFormatter)
	{
		$sysLogHandler->setFormatter($logFormatter);
		$logger->pushHandler($sysLogHandler);
		$this->logger = $logger;
	}

	public function get()
	{
		return $this->logger;
	}

} 