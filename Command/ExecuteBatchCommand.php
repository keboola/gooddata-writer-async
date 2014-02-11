<?php
namespace Keboola\GoodDataWriter\Command;

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
			->setDefinition(array(
				new InputArgument('batchId', InputArgument::REQUIRED, 'Batch id'),
				new InputOption('force', 'f', InputOption::VALUE_NONE, 'Force run the batch even if it is already finished')
			))
		;
	}

	protected function execute(InputInterface $input, OutputInterface $output)
	{
		$this->getContainer()->get('syrup.monolog.json_formatter')->setComponentName('gooddata-writer');

		$executor = $this->getContainer()->get('gooddata_writer.job_executor');
		$executor->runBatch($input->getArgument('batchId'), $input->getOption('force'));
	}

}