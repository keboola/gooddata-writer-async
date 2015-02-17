<?php
/**
 *
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-19
 *
 */

namespace Keboola\GoodDataWriter\Exception;

use Keboola\GoodDataWriter\GoodData\RestApi;

class RestApiException extends \Exception
{
    private $details;

    public function __construct($message, $details = null, $code = 0, \Exception $previous = null)
    {
        if ($details) {
            $this->setDetails($details);
        }

        parent::__construct($message, $code, $previous);
    }

    public function getDetails()
    {
        $result = [
            'error' => $this->getMessage(),
            'source' => 'Rest API'
        ];
        if (count($this->details)) {
            $result['details'] = $this->details;
        }
        return $result;
    }

    public function setDetails($details)
    {
        if (!is_array($details)) {
            $decode = json_decode($details, true);
            $details = $decode ? $decode : [$details];
        }

        $details = RestApi::parseError($details);
        foreach ($details as &$detail) {
            $detail = RestApi::parseError($detail);
        }

        $this->details = $details;
    }
}
