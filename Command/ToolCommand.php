<?php
namespace Keboola\GoodDataWriter\Command;

use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\GoodData\WebDav;
use Keboola\GoodDataWriter\Writer\AppConfiguration;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Keboola\GoodDataWriter\GoodData\Model;

class ToolCommand extends ContainerAwareCommand
{
	/**
	 * @var OutputInterface
	 */
	private $output;

	protected function configure()
	{
		$this
			->setName('gooddata-writer:tool')
			->setDescription('Tool for projects editing')
			->setDefinition(array(
				new InputArgument('pid', InputArgument::REQUIRED, 'Project id'),
				new InputArgument('name', InputArgument::REQUIRED, 'Name of dataset')
			))
		;
	}

	protected function execute(InputInterface $input, OutputInterface $output)
	{
		$this->output = $output;
		$this->loadTimeDimension($input->getArgument('pid'), $input->getArgument('name'));
	}

	public function loadTimeDimension($pid, $dimensionName)
	{
		$this->output->writeln('- Loading time dimension ' . $dimensionName);

		/** @var AppConfiguration $appConfiguration */
		$appConfiguration = $this->getContainer()->get('gooddata_writer.app_configuration');
		$sharedConfig = $this->getContainer()->get('gooddata_writer.shared_config');

		$domainUser = $sharedConfig->getDomainUser('keboola');

		/** @var RestApi $restApi */
		$restApi = $this->getContainer()->get('gooddata_writer.rest_api');
		$restApi->login($domainUser->username, $domainUser->password);


		$webDav = new WebDav($domainUser->username, $domainUser->password);

		$tmpFolderName = 'tool-'.uniqid();
		$tmpDir = $appConfiguration->tmpPath . '/' . $tmpFolderName;
		mkdir($tmpDir);
		$dimensionName = Model::getId($dimensionName);
		$tmpFolderDimension = $tmpDir . '/' . $dimensionName;
		$tmpFolderNameDimension = $tmpFolderName . '-' . $dimensionName;

		mkdir($tmpFolderDimension);
		$manifest = file_get_contents($appConfiguration->scriptsPath . '/time-dimension-manifest.json');
		$timeDimensionManifest = str_replace('%NAME%', $dimensionName, $manifest);
		file_put_contents($tmpFolderDimension . '/upload_info.json', $timeDimensionManifest);
		copy($appConfiguration->scriptsPath . '/time-dimension.csv', $tmpFolderDimension . '/' . $dimensionName . '.csv');
		$webDav->upload($tmpFolderDimension . '/upload_info.json', $tmpFolderNameDimension);
		$webDav->upload($tmpFolderDimension . '/' . $dimensionName . '.csv', $tmpFolderNameDimension);


		$restApi->loadData($pid, $tmpFolderNameDimension);
	}


}