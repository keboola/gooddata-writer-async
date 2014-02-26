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
use Keboola\GoodDataWriter\GoodData\RestApi;
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

		/**
		 * @var AppConfiguration $appConfiguration
		 */
		$appConfiguration = $this->getContainer()->get('gooddata_writer.app_configuration');

		$lock = new Lock(new \PDO(sprintf('mysql:host=%s;dbname=%s', $appConfiguration->db_host, $appConfiguration->db_name),
			$appConfiguration->db_user, $appConfiguration->db_password), 'CleanGoodDataCommand');
		if (!$lock->lock()) {
			return;
		}

		$log = $this->getContainer()->get('logger');
		$sharedConfig = $this->getContainer()->get('gooddata_writer.shared_config');

		/**
		 * @var RestApi
		 */
		$restApi = $this->getContainer()->get('gooddata_writer.rest_api');
		$restApi->login($appConfiguration->gd_username, $appConfiguration->gd_password);

		$pids = array();
		foreach ($sharedConfig->projectsToDelete() as $project) {
			try {
				$restApi->dropProject($project['pid']);
				$output->writeln(sprintf('Project %s deleted', $project['pid']));
			} catch (RestApiException $e) {
				$log->info('Could not delete project', array(
					'project' => $project,
					'exception' => $e->getDetails()
				));
			}
			$pids[] = $project['pid'];
		}
		$sharedConfig->markProjectsDeleted($pids);

		$uids = array();
		foreach ($sharedConfig->usersToDelete() as $user) {
			try {
				$restApi->dropUser($user['uid']);
				$output->writeln(sprintf('User %s deleted', $user['uid']));
			} catch (RestApiException $e) {
				$log->info('Could not delete user', array(
					'user' => $user,
					'exception' => $e->getDetails()
				));
			}
			$uids[] = $user['uid'];
		}
		$sharedConfig->markUsersDeleted($uids);

		$lock->unlock();
	}

}