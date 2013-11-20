<?php
namespace Keboola\GoodDataWriter\Command;

use Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\GoodDataWriter\Writer\JobExecutor;
use Keboola\GoodDataWriter\Writer\SharedConfig;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class DebugCommand extends ContainerAwareCommand
{


	protected function configure()
	{
		$this
			->setName('gooddata-writer:debug')
			->setDescription('Debug')
		;
	}

	protected function execute(InputInterface $input, OutputInterface $output)
	{
		$this->getContainer()->get('syrup.monolog.json_formatter')->setComponentName('gooddata-writer');

		$log = $this->getContainer()->get('logger');
		$mainConfig = $this->getContainer()->getParameter('gooddata_writer');

		$restApi = new \Keboola\GoodDataWriter\GoodData\RestApi(null, $this->getContainer()->get('logger'));
		$restApi->setCredentials($mainConfig['gd']['username'], $mainConfig['gd']['password']);


		/*$users = $restApi->get('/gdc/account/domains/keboola-devel/users');
		foreach ($users['accountSettings']['items'] as $i => $user) {
			echo ($i+1).' - '.$user['accountSetting']['login'] . ' - '.$user['accountSetting']['links']['self'].PHP_EOL;
		}die();*/

		$projects = $restApi->get('/gdc/md');
		$counter = 0;
		$counterAll = 0;
		foreach ($projects['about']['links'] as $project) {
			try {
				$counterAll++;
				$projectInfo = '"' . $counter . '","' . $project['title'] . '","' . $project['identifier'] . '","';

				$counter++;  echo $projectInfo.PHP_EOL;/*
				if (strstr($project['title'], '[Test] GoodData Writer -')) {
					$counter++;
					echo $projectInfo.PHP_EOL;
					$restApi->dropProject($project['identifier']);
				}/**/

				/*$usersInfo = array();
				$users = $restApi->get('/gdc/projects/' . $project['identifier'] . '/users');
				if (count($users['users']) == 2) {
					echo $projectInfo.PHP_EOL;
				}
				/**/


			} catch (\Keboola\GoodDataWriter\GoodData\RestApiException $e) {

			}

		}echo $counter . ' of ' . $counterAll . PHP_EOL;

	}

}