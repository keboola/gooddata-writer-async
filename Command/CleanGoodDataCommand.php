<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-05-22
 */

namespace Keboola\GoodDataWriter\Command;

use Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\GoodDataWriter\Writer\AppConfiguration;
use Keboola\GoodDataWriter\Writer\SharedConfig;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand,
	Symfony\Component\Console\Input\InputInterface,
	Symfony\Component\Console\Output\OutputInterface;
use Keboola\GoodDataWriter\GoodData\RestApi;

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

		/** @var AppConfiguration $appConfiguration */
		$appConfiguration = $this->getContainer()->get('gooddata_writer.app_configuration');
		/** @var SharedConfig $sharedConfig */
		$sharedConfig = $this->getContainer()->get('gooddata_writer.shared_config');

		$lock = $sharedConfig->getLock('CleanGoodDataCommand');
		if (!$lock->lock()) {
			return;
		}

		$log = $this->getContainer()->get('logger');


		/** @var RestApi */
		$restApi = $this->getContainer()->get('gooddata_writer.rest_api');
		$domainUser = $sharedConfig->getDomainUser($appConfiguration->gd_domain);
		$restApi->login($domainUser->username, $domainUser->password);

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

		if (count($pids)) {
			$sharedConfig->markProjectsDeleted($pids);
		}

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

		if (count($uids)) {
			$sharedConfig->markUsersDeleted($uids);
		}

		$lock->unlock();
	}

}