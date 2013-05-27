<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-05-22
 */

namespace Keboola\GoodDataWriter\Command;


class CleanGoodDataCommand
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
		$this->_log = $this->getContainer()->get('logger');
		$mainConfig = $this->getContainer()->getParameter('gooddata_writer');
		$this->_db = new \Zend_Db_Adapter_Pdo_Mysql(array(
			'host' => $mainConfig['db']['host'],
			'username' => $mainConfig['db']['user'],
			'password' => $mainConfig['db']['password'],
			'dbname' => $mainConfig['db']['name']
		));
		$this->_queue = new Queue($this->_db);

		$this->_sharedConfig = new SharedConfig(
			new StorageApiClient($mainConfig['shared_sapi']['token'], $mainConfig['shared_sapi']['url'])
		);

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

}