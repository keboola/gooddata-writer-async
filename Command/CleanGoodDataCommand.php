<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-05-22
 */

namespace Keboola\GoodDataWriter\Command;

use Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\GoodDataWriter\Writer\AppConfiguration;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand,
	Symfony\Component\Console\Input\InputInterface,
	Symfony\Component\Console\Output\OutputInterface;
use Keboola\GoodDataWriter\GoodData\RestApi,
	Keboola\StorageApi\Client as StorageApiClient,
	Keboola\GoodDataWriter\Writer\SharedConfig;
use Keboola\GoodDataWriter\Service\Lock;

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
		$this->getContainer()->get('syrup.monolog.json_formatter')->setComponentName('gooddata-writer');

		$gdWriterParams = $this->getContainer()->getParameter('gooddata_writer');
		$lock = new Lock(new \PDO(sprintf('mysql:host=%s;dbname=%s', $gdWriterParams['db']['host'], $gdWriterParams['db']['name']),
			$gdWriterParams['db']['user'], $gdWriterParams['db']['password']), 'CleanGoodDataCommand');
		if (!$lock->lock()) {
			return;
		}

		$log = $this->getContainer()->get('logger');
		/**
		 * @var AppConfiguration $appConfiguration
		 */
		$appConfiguration = $this->getContainer()->get('appConfiguration');
		$sharedConfig = new SharedConfig(
			new StorageApiClient($appConfiguration->sharedSapi_token, $appConfiguration->sharedSapi_url)
		);

		/**
		 * @var RestApi
		 */
		$restApi = $this->getContainer()->get('restApi');
		$restApi->login($appConfiguration->gd_username, $appConfiguration->gd_password);

		$pids = array();
		foreach ($sharedConfig->projectsToDelete() as $project) {
			try {
				$restApi->dropProject($project['pid']);
				$pids[] = $project['pid'];
				$output->writeln(sprintf('Project %s deleted', $project['pid']));
			} catch (RestApiException $e) {
				$log->alert('Could not delete project', array(
					'project' => $project,
					'exception' => $e->getDetails()
				));
			}
		}
		$sharedConfig->markProjectsDeleted($pids);

		$uids = array();
		foreach ($sharedConfig->usersToDelete() as $user) {
			try {
				$restApi->dropUser($user['uid']);
				$uids[] = $user['uid'];
				$output->writeln(sprintf('User %s deleted', $user['uid']));
			} catch (RestApiException $e) {
				$log->alert('Could not delete user', array(
					'user' => $user,
					'exception' => $e->getDetails()
				));
			}
		}
		$sharedConfig->markUsersDeleted($uids);

		$lock->unlock();
	}

}