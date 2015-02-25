<?php
namespace Keboola\GoodDataWriter\Command;

use Keboola\GoodDataWriter\Writer\JobExecutor;
use Keboola\GoodDataWriter\Writer\JobStorage;
use Keboola\GoodDataWriter\Writer\QueueUnavailableException;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class ExecuteBatchCommand extends ContainerAwareCommand
{


    protected function configure()
    {
        $this
            ->setName('gooddata-writer:execute-batch')
            ->setDescription('Execute selected batch from queue')
            ->setDefinition([
                new InputArgument('batchId', InputArgument::REQUIRED, 'Batch id'),
                new InputOption('force', 'f', InputOption::VALUE_NONE, 'Force run the batch even if it is already finished')
            ])
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        /** @var JobExecutor $executor */
        $executor = $this->getContainer()->get('gooddata_writer.job_executor');
        /** @var SharedStorage $sharedStorage */
        $sharedStorage = $this->getContainer()->get('gooddata_writer.shared_storage');
        /** @var JobStorage $jobStorage */
        $jobStorage = $this->getContainer()->get('gooddata_writer.job_storage');

        $jobs = $jobStorage->fetchBatch($input->getArgument('batchId'));
        if (!count($jobs)) {
            throw new \Exception(sprintf("Batch '%d' not found in Shared Storage", $input->getArgument('batchId')));
        }
        $batch = JobStorage::batchToApiResponse($input->getArgument('batchId'), $jobs);

        // Batch already executed?
        if (!$input->getOption('force') && $batch['status'] != JobStorage::JOB_STATUS_WAITING) {
            return;
        }

        // Lock
        $lock = $sharedStorage->getLock($batch['queueId']);
        if (!$lock->lock()) {
            throw new QueueUnavailableException($this->getContainer()->get('translator')->trans(
                'queue.in_use %1',
                ['%1' => $input->getArgument('batchId')]
            ));
        }

        foreach ($batch['jobs'] as $job) {
            $executor->run($job['id'], $input->getOption('force'));
        }

        $lock->unlock();
    }
}
