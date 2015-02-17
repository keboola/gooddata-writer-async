<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 13.02.14
 * Time: 9:04
 */

namespace Keboola\GoodDataWriter\GoodData;

use Keboola\GoodDataWriter\Monolog\Formatter\RestAPILogFormatter;
use Monolog\Handler\SyslogHandler;
use Monolog\Logger;

class RestApiLoggerConfigurator
{
    private $sysLogHandler;

    public function __construct(SyslogHandler $sysLogHandler, RestAPILogFormatter $logFormatter)
    {
        $this->sysLogHandler = $sysLogHandler;
        $this->sysLogHandler->setFormatter($logFormatter);
    }

    public function configure(Logger $logger)
    {
        $logger->pushHandler($this->sysLogHandler);
    }
}
