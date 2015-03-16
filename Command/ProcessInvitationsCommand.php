<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */
namespace Keboola\GoodDataWriter\Command;

use Keboola\GoodDataWriter\GoodData\InvitationsHandler;
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

        $startTime = time();
        do {
            $server = new \Fetch\Server('imap.gmail.com/ssl', 993);
            $server->setAuthentication($config['email'], $config['password']);

            /** @var InvitationsHandler $invitationsHandler */
            $invitationsHandler = $this->getContainer()->get('gooddata_writer.invitations_handler');
            $invitationsHandler->run();

            sleep(10);
        } while ((time() - $startTime) < 300);
    }
}
