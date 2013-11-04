<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-08-19
 */

namespace Keboola\GoodDataWriter\Service;


class QueueMessage
{
	private $_id;
	private $_body;
	private $_receiptHandle;
	private $_queueUrl;


	public function __construct($id, \stdClass $body, $receiptHandle, $queueUrl)
	{
		$this->_id = $id;
		$this->_body = $body;
		$this->_receiptHandle = $receiptHandle;
		$this->_queueUrl = $queueUrl;
	}

	public function getId()
	{
		return $this->_id;
	}

	/**
	 * @return \stdClass
	 */
	public function getBody()
	{
		return $this->_body;
	}

	public function getReceiptHandle()
	{
		return $this->_receiptHandle;
	}

	public function getQueueUrl()
	{
		return $this->_queueUrl;
	}

	/**
	 * @return int
	 */
	public function getRetryCount()
	{
		return isset($this->_body->retryCount) ? (int) $this->_body->retryCount : 0;
	}

	/**
	 * @return $this
	 */
	public function incrementRetries()
	{
		$this->_body->retryCount = isset($this->_body->retryCount) ? $this->_body->retryCount + 1 : 1;
		return $this;
	}

	public function toArray()
	{
		return array(
			'id' => $this->getId(),
			'body' => $this->getBody(),
		);
	}
} 