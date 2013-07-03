<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-05-22
 */

namespace Keboola\GoodDataWriter\Command;

use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand,
	Symfony\Component\Console\Input\InputArgument,
	Symfony\Component\Console\Input\InputInterface,
	Symfony\Component\Console\Input\InputOption,
	Symfony\Component\Console\Output\OutputInterface;
use Keboola\GoodDataWriter\GoodData\RestApi,
	Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\StorageApi\Client as StorageApiClient,
	Keboola\GoodDataWriter\Writer\SharedConfig;

class CleanGoodDataCommand extends ContainerAwareCommand
{

	protected function configure()
	{
		$this
			->setName('gooddata-writer:clean-gooddata')
			->setDescription('Clean obsolete GoodData projects and users')
		;
	}

	protected function execute(InputInterface $input, OutputInterface $output)
	{
		$log = $this->getContainer()->get('logger');
		$mainConfig = $this->getContainer()->getParameter('gooddata_writer');

		$sharedConfig = new SharedConfig(
			new StorageApiClient($mainConfig['shared_sapi']['token'], $mainConfig['shared_sapi']['url'])
		);

		$restApi = new RestApi(null, $log);

		$pids = array();
		foreach ($sharedConfig->projectsToDelete() as $project) {
			$env = $project['dev'] ? 'dev' : 'prod';
			$restApi->login($mainConfig['gd'][$env]['username'], $mainConfig['gd'][$env]['password']);
			$restApi->dropProject($project['pid']);
			$pids[] = $project['pid'];
		}
		$sharedConfig->markProjectsDeleted($pids);
		print_r($pids);

		$uids = array();
		foreach ($sharedConfig->usersToDelete() as $user) {
			$env = $user['dev'] ? 'dev' : 'prod';
			$restApi->login($mainConfig['gd'][$env]['username'], $mainConfig['gd'][$env]['password']);
			$restApi->dropUser($user['uid']);
			$uids[] = $user['uid'];
		}
		$sharedConfig->markUsersDeleted($uids);
		print_r($uids);die();
	}

}