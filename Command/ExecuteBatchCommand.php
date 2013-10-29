<?php
namespace Keboola\GoodDataWriter\Command;

use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\GoodDataWriter\Writer\JobExecutor,
	Keboola\GoodDataWriter\Writer\SharedConfig,
	Keboola\GoodDataWriter\Service\Queue,
	Keboola\GoodDataWriter\Service\QueueMessage;
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
				new InputOption('force', null, InputArgument::OPTIONAL, 'Force run the batch even if it is already finished')
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
		$executor->runBatch($input->getArgument('batchId'), $input->getOption('force'));

		$params = $this->getContainer()->getParameter('gooddata_writer');
		$sqsClient = \Aws\Sqs\SqsClient::factory(array(
			'key' => $params['aws']['access_key'],
			'secret' => $params['aws']['secret_key'],
			'region' => $params['aws']['region']
		));
	}

}