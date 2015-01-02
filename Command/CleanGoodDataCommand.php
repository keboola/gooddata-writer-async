<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-05-22
 */

namespace Keboola\GoodDataWriter\Command;

use Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\GoodDataWriter\Writer\SharedStorage;
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
		$gdConfig = $this->getContainer()->getParameter('gdwr_gd');
		/** @var SharedStorage $sharedStorage */
		$sharedStorage = $this->getContainer()->get('gooddata_writer.shared_storage');

		$lock = $sharedStorage->getLock('CleanGoodDataCommand');
		if (!$lock->lock()) {
			return;
		}

		$log = $this->getContainer()->get('logger');


		/** @var RestApi */
		$restApi = $this->getContainer()->get('gooddata_writer.rest_api');
		$domainUser = $sharedStorage->getDomainUser($gdConfig['domain']);
		$restApi->login($domainUser->username, $domainUser->password);

		$pids = array();
		foreach ($sharedStorage->projectsToDelete() as $project) {
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
			$sharedStorage->markProjectsDeleted($pids);
		}

		$uids = array();
		foreach ($sharedStorage->usersToDelete() as $user) {
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
			$sharedStorage->markUsersDeleted($uids);
		}

		$lock->unlock();
	}

}