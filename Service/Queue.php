<?php

namespace Keboola\GoodDataWriter\Service;


use Aws\Sqs\SqsClient;

class Queue
{

	/**
	 * @var SqsClient
	 */
	protected $client;
	protected $queueUrl;

	public function __construct($config)
	{
		if (!isset($config['access_key'])) {
			throw new \Exception("Key 'access_key' is missing from config");
		}
		if (!isset($config['secret_key'])) {
			throw new \Exception("Key 'secret_key' is missing from config");
		}
		if (!isset($config['region'])) {
			throw new \Exception("Key 'region' is missing from config");
		}
		if (!isset($config['queue_url'])) {
			throw new \Exception("Key 'queue_url' is missing from config");
		}

		$this->client = SqsClient::factory(array(
			'key' => $config['access_key'],
			'secret' => $config['secret_key'],
			'region' => $config['region']
		));
		$this->queueUrl = $config['queue_url'];
	}


	/**
	 */
	public function enqueue($body, $delay = 0)
	{
		$message = $this->client->sendMessage(array(
			'QueueUrl' => $this->queueUrl,
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
		$result = $this->client->receiveMessage(array(
			'QueueUrl' => $this->queueUrl,
			'WaitTimeSeconds' => 20,
			'MaxNumberOfMessages' => $messagesCount,
		));

		$queueUrl = $this->queueUrl;
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
		$this->client->deleteMessage(array(
			'QueueUrl' => $message->getQueueUrl(),
			'ReceiptHandle' => $message->getReceiptHandle(),
		));
	}

}
