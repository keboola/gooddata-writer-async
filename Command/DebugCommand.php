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
		$log = $this->getContainer()->get('logger');
		$mainConfig = $this->getContainer()->getParameter('gooddata_writer');

		$restApi = new \Keboola\GoodDataWriter\GoodData\RestApi(null, $this->getContainer()->get('logger'));
		$restApi->login($mainConfig['gd']['prod']['username'], $mainConfig['gd']['prod']['password']);


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
				if ($project['title'] == 'Keboola Academy BU1 (Milan@veverka.ca)')
				{
					if ($project['identifier']!='zpjkjhlp3by1ac8iepzbpgxe6q81d2ew') {
						//$restApi->dropProject($project['identifier']);
					}
					echo $project['identifier'].',';//echo $projectInfo.PHP_EOL;
					$counter++;
					//
				}
				/*if (strstr($project['title'], 'Keboola Academy RM1 -')) {
					$counter++;
					echo $projectInfo.PHP_EOL;
					//$restApi->dropProject($project['identifier']);
				}*/
				/*$usersInfo = array();
				$users = $restApi->get('/gdc/projects/' . $project['identifier'] . '/users');
				if (count($users['users']) == 2) {
					echo $projectInfo.PHP_EOL;
				}*/



				/*foreach ($users['users'] as $user) if ($user['user']['content']['email'] != 'gooddata@keboola.com' && strstr($user['user']['content']['email'], '@keboola.com')) {
					$usersInfo[] = substr($user['user']['content']['email'], 0, strpos($user['user']['content']['email'], '@'));
				}
				$projectInfo .= implode(',', $usersInfo) . '"';
				echo $projectInfo.PHP_EOL;
				//if (count($users['users']) == 2) *//*if (strstr($project['title'], 'Syrup'))*/

					//$restApi->dropProject($project['identifier']);
					//echo $users['users'][0]['user']['content']['email'].PHP_EOL.PHP_EOL;

			} catch (\Keboola\GoodDataWriter\GoodData\RestApiException $e) {

			}

		}echo $counter . ' of ' . $counterAll . PHP_EOL;

	}

}