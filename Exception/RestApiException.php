<?php
/**
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-19
 *
 */

namespace Keboola\GoodDataWriter\Exception;

use Keboola\Syrup\Exception\UserException;

class RestApiException extends UserException
{
    public function __construct($message, $details = null, $code = 0, \Exception $previous = null)
    {
        if ($details) {
            $this->setDetails($details);
        }

        parent::__construct($message, $previous);
    }

    public function setDetails($details)
    {
        if (!is_array($details)) {
            $decode = json_decode($details, true);
            $details = $decode ? $decode : [$details];
        }

        $details = self::parseError($details);
        foreach ($details as &$detail) {
            $detail = self::parseError($detail);
        }

        $this->data = $details;
    }

    public static function parseError($message)
    {
        if (isset($message['error']) && isset($message['error']['parameters']) && isset($message['error']['message'])) {
            $message['error']['message'] = vsprintf($message['error']['message'], $message['error']['parameters']);
            unset($message['error']['parameters']);
        }
        return $message;
    }
}
