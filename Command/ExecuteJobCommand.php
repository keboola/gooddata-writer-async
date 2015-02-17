<?php
namespace Keboola\GoodDataWriter\Command;

use Keboola\GoodDataWriter\Writer\JobExecutor;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class ExecuteJobCommand extends ContainerAwareCommand
{


    protected function configure()
    {
        $this
            ->setName('gooddata-writer:execute-job')
            ->setDescription('Execute selected job')
            ->setDefinition(array(
                new InputArgument('jobId', InputArgument::REQUIRED, 'Job id'),
                new InputOption('force', 'f', InputOption::VALUE_NONE, 'Force run the job even if it is already finished')
            ))
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        /** @var JobExecutor $executor */
        $executor = $this->getContainer()->get('gooddata_writer.job_executor');
        $executor->run($input->getArgument('jobId'), $input->getOption('force'));
    }
}
