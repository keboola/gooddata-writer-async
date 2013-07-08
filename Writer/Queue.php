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

	public function clearQueue($name)
	{
		$q = $this->getQueue($name);
		$queueId = (int)$this->_db->fetchOne('SELECT queue_id FROM queue WHERE queue_name = ?', $name);
		if ($queueId) {
			$this->_db->beginTransaction();
			$jobs = $this->_db->fetchCol("SELECT body FROM message WHERE queue_id = ?", $queueId);
			$this->_db->delete('message', array('queue_id=?' => $queueId));
			$this->_db->commit();
			return $jobs;
		}
	}

	public function fetchAllQueuesNamesOrderedByMessageAge()
	{
		return $this->_db->fetchCol("
			SELECT q.queue_name, MIN(m.created) as minTime
			FROM message m
			JOIN queue q ON (q.queue_id=m.queue_id)
			GROUP BY m.queue_id
			ORDER BY minTime ASC
		");
	}

	public function checkJobInQueue($id)
	{
		return ($this->_db->fetchOne('SELECT COUNT(*) FROM message WHERE body = ?', $id)) > 0;
	}

}
