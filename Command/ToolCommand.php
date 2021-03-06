<?php
namespace Keboola\GoodDataWriter\Command;

use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\GoodData\WebDav;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\Temp\Temp;

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

        $sharedStorage = $this->getContainer()->get('gooddata_writer.shared_storage');

        $domainUser = $sharedStorage->getDomainUser('keboola');

        /** @var RestApi $restApi */
        $restApi = $this->getContainer()->get('gooddata_writer.rest_api');
        $restApi->login($domainUser->username, $domainUser->password);

        /** @var Temp $temp */
        $temp = $this->getContainer()->get('syrup.temp');

        $webDav = new WebDav($domainUser->username, $domainUser->password);

        $tmpFolderName = 'tool-'.uniqid();
        $tmpDir = $temp->getTmpFolder() . '/' . $tmpFolderName;
        mkdir($tmpDir);
        $dimensionName = Model::getId($dimensionName);
        $tmpFolderDimension = $tmpDir . '/' . $dimensionName;
        $tmpFolderNameDimension = $tmpFolderName . '-' . $dimensionName;

        mkdir($tmpFolderDimension);
        $manifest = file_get_contents($this->getContainer()->getParameter('gdwr_scripts_path') . '/time-dimension-manifest.json');
        $timeDimensionManifest = str_replace('%NAME%', $dimensionName, $manifest);
        file_put_contents($tmpFolderDimension . '/upload_info.json', $timeDimensionManifest);
        copy($this->getContainer()->getParameter('gdwr_scripts_path') . '/time-dimension.csv', $tmpFolderDimension . '/' . $dimensionName . '.csv');
        $webDav->upload($tmpFolderDimension . '/upload_info.json', $tmpFolderNameDimension);
        $webDav->upload($tmpFolderDimension . '/' . $dimensionName . '.csv', $tmpFolderNameDimension);


        $restApi->loadData($pid, $tmpFolderNameDimension);
    }
}
