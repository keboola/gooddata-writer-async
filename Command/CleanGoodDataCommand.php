<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-05-22
 */

namespace Keboola\GoodDataWriter\Command;

use Keboola\GoodDataWriter\GoodData\RestApiException;
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

		$pids = array();
		foreach ($sharedConfig->projectsToDelete() as $project) {
			$env = $project['dev'] ? 'dev' : 'prod';
			$restApi = new RestApi($project['backendUrl'], $log);
			$restApi->login($mainConfig['gd'][$env]['username'], $mainConfig['gd'][$env]['password']);
			try {
				$restApi->dropProject($project['pid']);
				$pids[] = $project['pid'];
				$output->writeln(sprintf('Project %s deleted', $project['pid']));
			} catch (RestApiException $e) {
				$log->alert('Could nor delete project', array(
					'project' => $project,
					'exception' => $e
				));
			}
		}
		$sharedConfig->markProjectsDeleted($pids);

		$restApi = new RestApi(null, $log);
		$uids = array();
		foreach ($sharedConfig->usersToDelete() as $user) {
			$env = $user['dev'] ? 'dev' : 'prod';
			$restApi->login($mainConfig['gd'][$env]['username'], $mainConfig['gd'][$env]['password']);
			try {
				$restApi->dropUser($user['uid']);
				$uids[] = $user['uid'];
				$output->writeln(sprintf('User %s deleted', $user['uid']));
			} catch (RestApiException $e) {
				$log->alert('Could nor delete user', array(
					'user' => $user,
					'exception' => $e
				));
			}
		}
		$sharedConfig->markUsersDeleted($uids);
	}

}