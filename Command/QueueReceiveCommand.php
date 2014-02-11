<?php
namespace Keboola\GoodDataWriter\Command;

use Keboola\GoodDataWriter\Writer\AppConfiguration;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

use Keboola\GoodDataWriter\Service\Queue,
	Keboola\GoodDataWriter\Service\QueueMessage;
use Keboola\GoodDataWriter\Writer\JobCannotBeExecutedNowException;

class QueueReceiveCommand extends ContainerAwareCommand
{

	const MAX_RUN_TIME = 300;
	const MAX_EXECUTION_RETRIES = 4;

	/**
	 * @var Queue
	 */
	protected $queue;

	/**
	 * @var OutputInterface
	 */
	protected $output;


	/**
	 * Configure command, set parameters definition and help.
	 */
	protected function configure()
	{
		$this
			->setName('gooddata-writer:queue:receive')
			->setDefinition(array())
			->setDescription('Receive messages from queue - queue poll');
	}

	protected function execute(InputInterface $input, OutputInterface $output)
	{
		$this->getContainer()->get('syrup.monolog.json_formatter')->setComponentName('gooddata-writer');

		$this->queue = $this->getContainer()->get('gooddata_writer.queue');

		$this->output = $output;
		$startTime = time();
		do {
			foreach ($this->queue->receive() as $message) {
				$this->_processMessage($message);
			}
		} while ((time() - $startTime) < self::MAX_RUN_TIME);
	}



	protected function _processMessage(QueueMessage $message)
	{
		$log = $this->getContainer()->get('logger');
		$logData = array(
			'messageId' => $message->getId(),
			'batchId' => $message->getBody()->batchId,
			'projectId' => $message->getBody()->projectId,
			'writerId' => $message->getBody()->writerId
		);

		try {
			$this->output->writeln(sprintf('Received message: %s { batch: %s, project: %s, writer: %s }', $message->getId()
				, $message->getBody()->batchId, $message->getBody()->projectId, $message->getBody()->writerId));
			$log->info("Received message", $logData);
			$command = $this->getApplication()->find('gooddata-writer:execute-batch');

			$input = new \Symfony\Component\Console\Input\ArrayInput(array(
				$command->getName(),
				'batchId' => $message->getBody()->batchId,
			));
			$command->run($input, $this->output);

		} catch (JobCannotBeExecutedNowException $e) {
			// enqueue again
			$delaySecs = 60;
			$newMessageId = $this->queue->enqueue(array(
				'projectId' => $message->getBody()->projectId,
				'writerId' => $message->getBody()->writerId,
				'batchId' => $message->getBody()->batchId
			), $delaySecs);
			$log->info("Batch execution postponed", array_merge($logData, array(
				'newMessageId' => $newMessageId
			)));
			$this->output->writeln(sprintf("<info>%s</info>", $e->getMessage()));
		} catch(\Exception $e) {
			$message->incrementRetries();
			if ($message->getRetryCount() > self::MAX_EXECUTION_RETRIES) {
				$this->_errorMaximumRetriesExceeded($message->getBody()->batchId);
				$log->alert("Queue process error (Maximum retries exceeded)", array_merge($logData, array(
					'retryCount' => $message->getRetryCount(),
					'message' => $message->toArray(),
					'exception' => $e
				)));
			} else {
				// enqueue again
				$delaySecs = 60 * pow(2, $message->getRetryCount());
				$newMessageId = $this->queue->enqueue(array(
					'projectId' => $message->getBody()->projectId,
					'writerId' => $message->getBody()->writerId,
					'batchId' => $message->getBody()->batchId
				), $delaySecs);
				$log->err("Queue process error", array_merge($logData, array(
					'newMessageId' => $newMessageId,
					'retryCount' => $message->getRetryCount(),
					'delaySecs' => $delaySecs,
					'message' => $message->toArray(),
					'exception' => $e
				)));
			}
			$this->output->writeln(sprintf("<error>%s</error>", $e->getMessage()));
		}
		$this->queue->deleteMessage($message);
		$log->info("Deleted message", array(
			'messagedId' => $message->getId(),
			'batchId' => $message->getBody()->batchId,
			'projectId' => $message->getBody()->projectId,
			'writerId' => $message->getBody()->writerId
		));
	}

	protected function _errorMaximumRetriesExceeded($batchId)
	{
		$sharedConfig = $this->getContainer()->get('gooddata_writer.shared_config');

		$batch = $sharedConfig->fetchBatch($batchId);
		if (!$batch) {
			return;
		}

		foreach ($batch as $job) {
			$sharedConfig->saveJob($job['id'], array(
				'status' => 'error',
				'error' => 'Maximum execution retries exceeded.'
			));
		}
	}



}