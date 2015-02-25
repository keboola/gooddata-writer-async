<?php
namespace Keboola\GoodDataWriter\Command;

use Keboola\GoodDataWriter\Writer\JobStorage;
use Keboola\GoodDataWriter\Writer\QueueUnavailableException;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

use Keboola\GoodDataWriter\Service\Queue;
use Keboola\GoodDataWriter\Service\QueueMessage;

class QueueReceiveCommand extends ContainerAwareCommand
{

    const MAX_RUN_TIME = 300;
    const MAX_EXECUTION_RETRIES = 5;

    /**
     * @var Queue
     */
    protected $queue;

    /**
     * @var \Symfony\Component\Translation\Translator
     */
    private $translator;

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
            ->setDefinition([])
            ->setDescription('Receive messages from queue - queue poll');
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $this->queue = $this->getContainer()->get('gooddata_writer.jobs_queue');
        $this->translator = $this->getContainer()->get('translator');

        $this->output = $output;
        $startTime = time();
        do {
            foreach ($this->queue->receive() as $message) {
                $this->processMessage($message);
            }
        } while ((time() - $startTime) < self::MAX_RUN_TIME);
    }



    protected function processMessage(QueueMessage $message)
    {
        $log = $this->getContainer()->get('logger');
        $logData = [
            'messageId' => $message->getId(),
            'batchId' => $message->getBody()->batchId,
            'projectId' => $message->getBody()->projectId,
            'writerId' => $message->getBody()->writerId
        ];

        try {
            $this->output->writeln(sprintf(
                'Received message: %s { batch: %s, project: %s, writer: %s }',
                $message->getId(),
                $message->getBody()->batchId,
                $message->getBody()->projectId,
                $message->getBody()->writerId
            ));
            $log->info($this->translator->trans('queue.message_received'), $logData);

            /** @var \Keboola\GoodDataWriter\Command\ExecuteBatchCommand $command */
            $command = $this->getApplication()->find('gooddata-writer:execute-batch');
            $input = [
                $command->getName(),
                'batchId' => $message->getBody()->batchId
            ];
            if (!empty($message->getBody()->force)) {
                $input['--force'] = true;
            }
            $command->run(new \Symfony\Component\Console\Input\ArrayInput($input), $this->output);

        } catch (QueueUnavailableException $e) {
            // enqueue again
            $delaySecs = 60;
            $newMessageId = $this->queue->enqueue($message->getBody(), $delaySecs);
            $log->info($this->translator->trans('queue.batch_postponed'), array_merge($logData, [
                'newMessageId' => $newMessageId
            ]));
            $this->output->writeln(sprintf("<info>%s</info>", $e->getMessage()));
        } catch (\Exception $e) {
            $message->setForceRun();
            $message->incrementRetries();
            if ($message->getRetryCount() > self::MAX_EXECUTION_RETRIES) {
                $this->errorMaximumRetriesExceeded($message->getBody()->batchId);
                $log->alert($this->translator->trans('queue.error_max_retries'), array_merge($logData, [
                    'retryCount' => $message->getRetryCount(),
                    'message' => $message->toArray(),
                    'exception' => $e
                ]));
            } else {
                // enqueue again
                $delaySecs = 60 * pow(2, $message->getRetryCount());
                if ($delaySecs > 900) {
                    $delaySecs = 900;
                }
                $newMessageId = $this->queue->enqueue($message->getBody(), $delaySecs);
                $log->err($this->translator->trans('queue.error'), array_merge($logData, [
                    'newMessageId' => $newMessageId,
                    'retryCount' => $message->getRetryCount(),
                    'delaySecs' => $delaySecs,
                    'message' => $message->toArray(),
                    'exception' => $e
                ]));
            }
            $this->output->writeln(sprintf("<error>%s</error>", $e->getMessage()));
        }
        $this->queue->deleteMessage($message);
        $log->info($this->translator->trans('queue.message_deleted'), [
            'messagedId' => $message->getId(),
            'batchId' => $message->getBody()->batchId,
            'projectId' => $message->getBody()->projectId,
            'writerId' => $message->getBody()->writerId
        ]);
    }

    protected function errorMaximumRetriesExceeded($batchId)
    {
        $jobStorage = $this->getContainer()->get('gooddata_writer.job_storage');

        $batch = $jobStorage->fetchBatch($batchId);
        if (!$batch) {
            return;
        }

        foreach ($batch as $job) {
            $jobStorage->saveJob($job['id'], [
                'status' => JobStorage::JOB_STATUS_ERROR,
                'error' => $this->translator->trans('error.max_retries_exceeded')
            ]);
        }
    }
}
