<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-05-22
 */

namespace Keboola\GoodDataWriter\Command;

use Doctrine\DBAL\Connection;
use Keboola\GoodDataWriter\Exception\RestApiException;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Keboola\Syrup\Service\Db\Lock;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
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

        /** @var Connection $conn */
        $conn = $this->getContainer()->get('doctrine.dbal.lock_connection');
        $conn->exec('SET wait_timeout = 31536000;');
        $lock = new Lock($conn, 'CleanGoodDataCommand');

        if (!$lock->lock()) {
            return;
        }

        $log = $this->getContainer()->get('logger');


        /** @var RestApi */
        $restApi = $this->getContainer()->get('gooddata_writer.rest_api');
        $domainUser = $sharedStorage->getDomainUser($gdConfig['domain']);
        $restApi->login($domainUser->username, $domainUser->password);

        $pids = [];
        foreach ($sharedStorage->projectsToDelete() as $project) {
            try {
                $restApi->dropProject($project['pid']);
                $output->writeln(sprintf('Project %s deleted', $project['pid']));
            } catch (RestApiException $e) {
                $log->info('Could not delete project', [
                    'project' => $project,
                    'exception' => $e->getDetails()
                ]);
            }
            $pids[] = $project['pid'];
        }

        if (count($pids)) {
            $sharedStorage->markProjectsDeleted($pids);
        }

        $uids = [];
        foreach ($sharedStorage->usersToDelete() as $user) {
            try {
                $restApi->dropUser($user['uid']);
                $output->writeln(sprintf('User %s deleted', $user['uid']));
            } catch (RestApiException $e) {
                $log->info('Could not delete user', [
                    'user' => $user,
                    'exception' => $e->getDetails()
                ]);
            }
            $uids[] = $user['uid'];
        }

        if (count($uids)) {
            $sharedStorage->markUsersDeleted($uids);
        }

        $lock->unlock();
    }
}
