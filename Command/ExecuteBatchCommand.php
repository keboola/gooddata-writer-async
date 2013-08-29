<?php
namespace Keboola\GoodDataWriter\Command;

use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\GoodDataWriter\Writer\JobExecutor;
use Keboola\GoodDataWriter\Writer\SharedConfig;
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
				new InputArgument('batchId', InputArgument::REQUIRED, 'Batch id')
			))
		;
	}

	protected function execute(InputInterface $input, OutputInterface $output)
	{
		$mainConfig = $this->getContainer()->getParameter('gooddata_writer');
		$sharedConfig = new SharedConfig(
			new StorageApiClient(
				$mainConfig['shared_sapi']['token'],
				$mainConfig['shared_sapi']['url'],
				$mainConfig['user_agent']
			)
		);

		$executor = new JobExecutor($sharedConfig, $this->getContainer());
		$executor->runBatch($input->getArgument('batchId'));
	}

}