<?php
namespace Keboola\GoodDataWriter\Command;

use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\GoodDataWriter\Writer\JobExecutor;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class WorkerDebugCommand extends ContainerAwareCommand
{


	protected function configure()
	{
		$this
			->setName('wr-gooddata:worker-debug')
			->setDescription('Queue worker Debug')
			->setDefinition(array(
				new InputArgument('job', InputArgument::REQUIRED, 'Job id')
			))
		;
	}

	protected function execute(InputInterface $input, OutputInterface $output)
	{
		$log = $this->getContainer()->get('logger');
		$mainConfig = $this->getContainer()->getParameter('gd_writer');
		$sapiSharedConfig = new StorageApiClient($mainConfig['shared_sapi']['token'], $mainConfig['shared_sapi']['url']);

		$executor = new JobExecutor($sapiSharedConfig, $log, $this->getContainer());
		$executor->runJob($input->getArgument('job'));

	}

}