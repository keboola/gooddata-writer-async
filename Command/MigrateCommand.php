<?php
namespace Keboola\GoodDataWriter\Command;

use Keboola\GoodDataWriter\GoodData\CLToolApi;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\GoodDataWriter\GoodData\WebDav;
use Keboola\GoodDataWriter\Job\UploadDateDimension;
use Keboola\GoodDataWriter\Service\JsonFormatter;
use Keboola\GoodDataWriter\Service\S3Client;
use Keboola\GoodDataWriter\Writer\AppConfiguration;
use Keboola\GoodDataWriter\Writer\Configuration;
use Keboola\GoodDataWriter\Writer\SharedConfig;
use Keboola\StorageApi\Client;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Process\Process;
use Syrup\ComponentBundle\Service\Encryption\EncryptorFactory;

class MigrateCommand extends ContainerAwareCommand
{

	protected function configure()
	{
		$this
			->setName('migrate')
			->setDescription('Migrate')
		;
	}

	protected function execute(InputInterface $input, OutputInterface $output)
	{
		/** @var SharedConfig $sharedConfig */
		$sharedConfig = $this->getContainer()->get('gooddata_writer.shared_config');
		$sharedConfig->migrate();
	}

}