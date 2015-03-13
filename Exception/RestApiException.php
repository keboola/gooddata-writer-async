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
    public function __construct($message, \Exception $previous = null, $data = null)
    {
        parent::__construct($message, $previous);
        if ($data) {
            $this->setData($data);
        }
    }

    public function setData(array $data)
    {
        $data = self::parseError($data);
        foreach ($data as &$d) {
            $d = self::parseError($d);
        }

        $this->data = $data;
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
