<?php

namespace Keboola\GoodDataWriter\Service;


use Aws\Sqs\SqsClient;
use Keboola\GoodDataWriter\Writer\AppConfiguration;

class Queue
{

	/**
	 * @var SqsClient
	 */
	protected $client;
	protected $queueUrl;

	public function __construct(AppConfiguration $appConfiguration)
	{
		$this->client = SqsClient::factory(array(
			'key' => $appConfiguration->aws_accessKey,
			'secret' => $appConfiguration->aws_secretKey,
			'region' => $appConfiguration->aws_region
		));
		$this->queueUrl = $appConfiguration->aws_jobsSqsUrl;
	}


	/**
	 * @param array $body
	 * @param int $delay
	 * @return
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
