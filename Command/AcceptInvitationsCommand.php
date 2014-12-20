<?php
namespace Keboola\GoodDataWriter\Command;

use Keboola\GoodDataWriter\GoodData\InvitationsHandler;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class AcceptInvitationsCommand extends ContainerAwareCommand
{


	protected function configure()
	{
		$this
			->setName('gooddata-writer:accept-invitations')
			->setDescription('Poll email and accept incoming invitations')
		;
	}

	protected function execute(InputInterface $input, OutputInterface $output)
	{
		/** @var InvitationsHandler $handler */
		$handler = $this->getContainer()->get('gooddata_writer.invitations_handler');
		$handler->run();
	}

}