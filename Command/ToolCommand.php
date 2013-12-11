<?php
namespace Keboola\GoodDataWriter\Command;

use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\GoodData\WebDav;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

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

		$mainConfig = $this->getContainer()->getParameter('gooddata_writer');

		$restApi = new RestApi(null, $this->getContainer()->get('logger'));
		$restApi->setCredentials($mainConfig['gd']['username'], $mainConfig['gd']['password']);


		$webDav = new WebDav($mainConfig['gd']['username'], $mainConfig['gd']['password']);

		$tmpFolderName = 'tool-'.uniqid();
		$tmpDir = $mainConfig['tmp_path'] . '/' . $tmpFolderName;
		mkdir($tmpDir);
		$dimensionName = RestApi::gdName($dimensionName);
		$tmpFolderDimension = $tmpDir . '/' . $dimensionName;
		$tmpFolderNameDimension = $tmpFolderName . '-' . $dimensionName;

		mkdir($tmpFolderDimension);
		$manifest = file_get_contents($mainConfig['scripts_path'] . '/time-dimension-manifest.json');
		$timeDimensionManifest = str_replace('%NAME%', $dimensionName, $manifest);
		file_put_contents($tmpFolderDimension . '/upload_info.json', $timeDimensionManifest);
		copy($mainConfig['scripts_path'] . '/time-dimension.csv', $tmpFolderDimension . '/data.csv');
		$webDav->upload($tmpFolderDimension, $tmpFolderNameDimension, $tmpFolderDimension . '/upload_info.json', $tmpFolderDimension . '/data.csv');


		$restApi->loadData($pid, $tmpFolderNameDimension);
	}


}