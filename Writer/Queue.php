<?php

namespace Keboola\GoodDataWriterBundle\Writer;

use Keboola\StorageApi\Event as StorageApiEvent,
	Keboola\StorageApi\Table as StorageApiTable;

class Queue
{

	/**
	 * @var \Zend_Db_Adapter_Abstract
	 */
	protected $_db;

	public function __construct(\Zend_Db_Adapter_Abstract $db)
	{
		$this->_db = $db;
	}

	public function enqueueJob($job)
	{
		$queue = $this->_getQueueForJob($job);
		$queue->send($job['id']);
	}

	/**
	 * @param $job
	 * @return \Zend_Queue
	 */
	private function _getQueueForJob($job)
	{
		$queue = new \Zend_Queue(
			'Db',
			array(
				'name' => $job['projectId'] . "-" . $job['writerId'],
				'dbAdapter' => $this->_db,
				'options' => array(
					\Zend_Db_Select::FOR_UPDATE => true,
				),
			)
		);
		return $queue;
	}

}