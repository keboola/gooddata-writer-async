<?php
namespace Keboola\GoodDataWriterBundle\Command;

use Keboola\GoodDataWriterBundle\Service\Lock,
	Keboola\StorageApi\Client;
use Keboola\GoodDataWriterBundle\Writer\JobExecutor;
use Monolog\Logger;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class WorkerCommand extends ContainerAwareCommand
{
	protected $_db;

	/**
	 * @var Client
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



	public function __construct($name = null, \Zend_Db_Adapter_Abstract $db, Client $sharedConfig, Logger $log)
	{
		parent::__construct($name);
		$this->_db = $db;
		$this->_sapiSharedConfig = $sharedConfig;
		$this->_log = $log;
	}


	protected function configure()
	{
		$this
			->setName('gooddata:worker')
			->setDescription('Queue worker')
		;
	}

	protected function execute(InputInterface $input, OutputInterface $output)
	{
		$this->_output = $output;

		// process messages from first queue
		foreach ($this->_fetchAllQueuesNamesOrderedByMessageAge() as $queueName) {
			$lock = $this->_getLock('queue-' . $queueName);
			if (!$lock->lock()) {
				continue; // locked
			}

			$this->_receiveQueue($queueName);

			$lock->unlock();
			break;
		}
	}

	protected function _fetchAllQueuesNamesOrderedByMessageAge()
	{
		return $this->_db->fetchCol("
			SELECT q.queue_name, MIN(m.created) as minTime
			FROM message m
			JOIN queue q ON (q.queue_id=m.queue_id)
			WHERE m.handle is NULL
			GROUP BY m.queue_id
			ORDER BY minTime ASC
		");
	}

	/**
	 * @param $queueName
	 * @param int $maxMessagesCount
	 */
	protected function _receiveQueue($queueName, $maxMessagesCount = 10)
	{
		$this->_output->writeln('Receiving queue: ' . $queueName);
		$queue = $this->_getQueue($queueName);

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
		// fetch job
		$this->_output->writeln('Executing job: ' . $jobId);

		$executor = new JobExecutor($this->getContainer()->getParameter(''), $this->_sapiSharedConfig, $this->_log);
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

	/**
	 * @param $queueName
	 * @return \Zend_Queue
	 */
	protected function _getQueue($queueName)
	{
		return new \Zend_Queue(
			'Db',
			array(
				'name' => $queueName,
				'dbAdapter' => $this->_db,
				'options' => array(
					\Zend_Db_Select::FOR_UPDATE => true,
				),
			)
		);
	}
}