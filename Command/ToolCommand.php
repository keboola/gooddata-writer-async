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
				new InputArgument('name', InputArgument::OPTIONAL, 'Name of dataset')
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

		/**
		 * @var AppConfiguration $appConfiguration
		 */
		$appConfiguration = $this->getContainer()->get('appConfiguration');

		/**
		 * @var RestApi
		 */
		$restApi = $this->getContainer()->get('restApi');
		$restApi->login($appConfiguration->gd_username, $appConfiguration->gd_password);


		$webDav = new WebDav($appConfiguration->gd_username, $appConfiguration->gd_password);

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
		copy($appConfiguration->scriptsPath . '/time-dimension.csv', $tmpFolderDimension . '/data.csv');
		$webDav->upload($tmpFolderDimension, $tmpFolderNameDimension, $tmpFolderDimension . '/upload_info.json', $tmpFolderDimension . '/data.csv');


		$restApi->loadData($pid, $tmpFolderNameDimension, $dimensionName);
	}


}