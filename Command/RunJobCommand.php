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

class RunJobCommand extends ContainerAwareCommand
{


	protected function configure()
	{
		$this
			->setName('gooddata-writer:run-job')
			->setDescription('Run selected job from queue')
			->setDefinition(array(
				new InputArgument('job', InputArgument::REQUIRED, 'Job id')
			))
		;
	}

	protected function execute(InputInterface $input, OutputInterface $output)
	{
		$log = $this->getContainer()->get('logger');
		$mainConfig = $this->getContainer()->getParameter('gooddata_writer');
		$sharedConfig = new SharedConfig(
			new StorageApiClient($mainConfig['shared_sapi']['token'], $mainConfig['shared_sapi']['url'])
		);

		$db = new \Zend_Db_Adapter_Pdo_Mysql(array(
			'host' => $mainConfig['db']['host'],
			'username' => $mainConfig['db']['user'],
			'password' => $mainConfig['db']['password'],
			'dbname' => $mainConfig['db']['name']
		));
		$db->delete('message', array('body=?' => $input->getArgument('job')));

		$executor = new JobExecutor($sharedConfig, $log, $this->getContainer());
		$executor->runJob($input->getArgument('job'));

	}

}