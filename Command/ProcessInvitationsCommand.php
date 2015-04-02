<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */
namespace Keboola\GoodDataWriter\Command;

use Doctrine\DBAL\Connection;
use Keboola\GoodDataWriter\GoodData\InvitationsHandler;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Keboola\Syrup\Service\Db\Lock;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class ProcessInvitationsCommand extends ContainerAwareCommand
{

    protected function configure()
    {
        $this
            ->setName('gooddata-writer:process-invitations')
            ->setDescription('Poll email and accept incoming invitations')
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $config = $this->getContainer()->getParameter('gdwr_invitations');

        /** @var SharedStorage $sharedStorage */
        $sharedStorage = $this->getContainer()->get('gooddata_writer.shared_storage');
        $domainUser = $sharedStorage->getDomainUser($config['domain']);

        /** @var Connection $conn */
        $conn = $this->getContainer()->get('doctrine.dbal.lock_connection');
        $conn->exec('SET wait_timeout = 31536000;');
        $lock = new Lock($conn, 'ProcessInvitations');
        if (!$lock->lock()) {
            return;
        }

        $startTime = time();
        do {
            /** @var InvitationsHandler $invitationsHandler */
            $invitationsHandler = $this->getContainer()->get('gooddata_writer.invitations_handler');
            $invitationsHandler->run(
                $config['email'],
                $config['password'],
                $domainUser->username,
                $domainUser->password
            );

            sleep(10);
        } while ((time() - $startTime) < 300);

        $lock->unlock();
    }
}
