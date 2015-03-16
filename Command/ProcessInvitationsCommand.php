<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */
namespace Keboola\GoodDataWriter\Command;

use Keboola\GoodDataWriter\GoodData\InvitationsHandler;
use Keboola\GoodDataWriter\Writer\SharedStorage;
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
    }
}
