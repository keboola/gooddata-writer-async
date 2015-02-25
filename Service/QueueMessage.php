<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-08-19
 */

namespace Keboola\GoodDataWriter\Service;

class QueueMessage
{
    private $id;
    private $body;
    private $receiptHandle;
    private $queueUrl;


    public function __construct($id, \stdClass $body, $receiptHandle, $queueUrl)
    {
        $this->id = $id;
        $this->body = $body;
        $this->receiptHandle = $receiptHandle;
        $this->queueUrl = $queueUrl;
    }

    public function getId()
    {
        return $this->id;
    }

    /**
     * @return \stdClass
     */
    public function getBody()
    {
        return $this->body;
    }

    public function getReceiptHandle()
    {
        return $this->receiptHandle;
    }

    public function getQueueUrl()
    {
        return $this->queueUrl;
    }

    /**
     * @return int
     */
    public function getRetryCount()
    {
        return isset($this->body->retryCount) ? (int) $this->body->retryCount : 0;
    }

    /**
     * @return $this
     */
    public function incrementRetries()
    {
        $this->body->retryCount = isset($this->body->retryCount) ? $this->body->retryCount + 1 : 1;
        return $this;
    }

    /**
     *
     */
    public function setForceRun()
    {
        $this->body->force = 1;
        return $this;
    }

    public function toArray()
    {
        return [
            'id' => $this->getId(),
            'body' => $this->getBody()
        ];
    }
}
