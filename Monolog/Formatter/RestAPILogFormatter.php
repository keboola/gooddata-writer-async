<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 05.02.14
 * Time: 9:00
 */

namespace Keboola\GoodDataWriter\Monolog\Formatter;

use Monolog\Formatter\JsonFormatter;
use Syrup\ComponentBundle\Monolog\Uploader\SyrupS3Uploader;

class RestAPILogFormatter extends JsonFormatter
{
    /**
     * @var SyrupS3Uploader
     */
    private $uploader;

    public function __construct(SyrupS3Uploader $uploader)
    {
        $this->uploader = $uploader;
    }

    public function format(array $record)
    {
        if (isset($record['context']) && !count($record['context'])) {
            unset($record['context']);
        }
        if (isset($record['extra']) && !count($record['extra'])) {
            unset($record['extra']);
        }
        if (isset($record['level'])) {
            unset($record['level']);
        }
        if (isset($record['level_name'])) {
            unset($record['level_name']);
        }
        if (isset($record['datetime'])) {
            unset($record['datetime']);
        }

        if (isset($record['context'])) {
            $context = $record['context'];
            unset($record['context']);
            $record = array_merge($record, $context);
        }

        if (isset($record['request']['response']['body'])) {
            $decodedBody = json_decode($record['request']['response']['body'], true);
            if ($decodedBody) {
                $record['request']['response']['body'] = $decodedBody;
            }
        }

        if (isset($record['request']['params']['postUserLogin']['password'])) {
            $record['request']['params']['postUserLogin']['password'] = '***';
        }
        if (isset($record['request']['params']['accountSetting']['password'])) {
            $record['request']['params']['accountSetting']['password'] = '***';
        }
        if (isset($record['request']['params']['accountSetting']['verifyPassword'])) {
            $record['request']['params']['accountSetting']['verifyPassword'] = '***';
        }

        if (isset($record['request'])) {
            $jsonRequest = json_encode($record['request'], JSON_PRETTY_PRINT);
            if (strlen($jsonRequest) > 1000) {
                $s3file = null;
                if ($record['jobId']) {
                    $s3file .= $record['jobId'] . '/';
                }
                $s3file .= time() . '-' . uniqid() . '.json';
                $record['request'] = $this->uploader->uploadString($s3file, $jsonRequest);
            }
        }

        return json_encode($record);
    }
}
