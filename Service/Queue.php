<?php

namespace Keboola\GoodDataWriter\Service;


use Aws\Sqs\SqsClient;

class Queue
{

	/**
	 * @var SqsClient
	 */
	protected $_client;
	protected $_queueUrl;

	public function __construct(SqsClient $client, $queueUrl)
	{
		$this->_client = $client;
		$this->_queueUrl = $queueUrl;
	}


	/**
	 * @param array $body
	 * @param int $delay
	 * @return
	 */
	public function enqueue($body, $delay = 0)
	{
		$message = $this->_client->sendMessage(array(
			'QueueUrl' => $this->_queueUrl,
			'MessageBody' => json_encode($body),
			'DelaySeconds' => $delay,
		));
		return $message['MessageId'];
	}

	/**
	 * @param int $messagesCount
	 * @return array of QueueMessage
	 */
	public function receive($messagesCount = 1)
	{
		$result = $this->_client->receiveMessage(array(
			'QueueUrl' => $this->_queueUrl,
			'WaitTimeSeconds' => 20,
			'MaxNumberOfMessages' => $messagesCount,
		));
		$messages = $result['Messages'];

		$queueUrl = $this->_queueUrl;
		return array_map(function($message) use ($queueUrl) {
			return new QueueMessage(
				$message['MessageId'],
				json_decode($message['Body']),
				$message['ReceiptHandle'],
				$queueUrl
			);
		}, (array) $result['Messages']);
	}


	public function deleteMessage(QueueMessage $message)
	{
		$this->_client->deleteMessage(array(
			'QueueUrl' => $message->getQueueUrl(),
			'ReceiptHandle' => $message->getReceiptHandle(),
		));
	}

}
