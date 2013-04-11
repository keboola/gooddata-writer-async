<?php

namespace Keboola\GoodDataWriter\Writer;

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
		$queue = $this->getQueue($job['projectId'] . "-" . $job['writerId']);
		$queue->send($job['id']);
	}

	/**
	 * @param $name
	 * @return \Zend_Queue
	 */
	public function getQueue($name)
	{
		$queue = new \Zend_Queue(
			'Db',
			array(
				'name' => $name,
				'dbAdapter' => $this->_db,
				'options' => array(
					\Zend_Db_Select::FOR_UPDATE => true,
				),
			)
		);
		return $queue;
	}

	public function fetchAllQueuesNamesOrderedByMessageAge()
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

}