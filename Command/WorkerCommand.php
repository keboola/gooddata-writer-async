<?php
namespace Keboola\GoodDataWriterBundle\Command;

use Keboola\GoodDataWriterBundle\Service\Lock,
	Keboola\StorageApi\Client as StorageApiClient;
use Keboola\GoodDataWriterBundle\Writer\JobExecutor,
	Keboola\GoodDataWriterBundle\Writer\Queue;
use Monolog\Logger;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class WorkerCommand extends ContainerAwareCommand
{
	/**
	 * @var \Zend_Db_Adapter_Abstract
	 */
	protected $_db;
	/**
	 * @var Queue
	 */
	protected $_queue;
	/**
	 * @var StorageApiClient
	 */
	protected $_sapiSharedConfig;
	/**
	 * @var Logger
	 */
	protected $_log;
	/**
	 * @var \Symfony\Component\Console\Output\OutputInterface
	 */
	protected $_output;



	protected function configure()
	{
		$this
			->setName('wr-gooddata:worker')
			->setDescription('Queue worker')
		;
	}

	protected function execute(InputInterface $input, OutputInterface $output)
	{
		$this->_log = $this->getContainer()->get('logger');
		$mainConfig = $this->getContainer()->getParameter('gd_writer');
		$this->_db = new \Zend_Db_Adapter_Pdo_Mysql(array(
			'host' => $mainConfig['db']['host'],
			'username' => $mainConfig['db']['user'],
			'password' => $mainConfig['db']['password'],
			'dbname' => $mainConfig['db']['name']
		));
		$this->_queue = new Queue($this->_db);
		$this->_sapiSharedConfig = new StorageApiClient($mainConfig['shared_sapi']['token'], $mainConfig['shared_sapi']['url']);

		$this->_output = $output;

		// process messages from first queue
		foreach ($this->_queue->fetchAllQueuesNamesOrderedByMessageAge() as $queueName) {
			$lock = $this->_getLock('queue-' . $queueName);
			if (!$lock->lock()) {
				continue; // locked
			}

			$this->_receiveQueue($queueName);

			$lock->unlock();
			break;
		}
	}

	/**
	 * @param $queueName
	 * @param int $maxMessagesCount
	 */
	protected function _receiveQueue($queueName, $maxMessagesCount = 10)
	{
		$this->_output->writeln('Receiving queue: ' . $queueName);
		$queue = $this->_queue->getQueue($queueName);

		foreach ($queue->receive($maxMessagesCount) as $message) {
			try {
				$this->_executeJob($message->body);
			} catch (\Exception $e) {
				$this->_log->alert('Job execution error', array(
					'jobId' => $message->body,
					'queueName' => $queueName,
					'exception' => $e,
				));
				$this->_output->writeln('Job execution error:' . $e->getMessage());
			}
			$queue->deleteMessage($message);
		}
	}

	/**
	 * @param $jobId
	 */
	protected function _executeJob($jobId)
	{
		$this->_output->writeln('Executing job: ' . $jobId);

		$executor = new JobExecutor($this->_sapiSharedConfig, $this->_log, $this->getContainer());
		$executor->runJob($jobId);
	}

	/**
	 * @param $lockName
	 * @return Lock
	 */
	protected function _getLock($lockName)
	{
		return new Lock($this->_db, $lockName);
	}

}