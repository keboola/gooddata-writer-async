<?php
/**
 * Script access lock
 * Inspired by:
 *  - http://www.mysqlperformanceblog.com/2009/10/14/watch-out-for-your-cron-jobs/
 *  - http://www.phpdeveloper.org.uk/mysql-named-locks/
 *
 * User: Martin Halamíček
 * Date: 27.12.11
 * Time: 9:45
 */

namespace Keboola\GoodDataWriter\Service;

class Lock
{

	/**
	 * @var \Zend_Db_Adapter_Abstract
	 */
	protected $_db;
	protected $_lockName;


	/**
	 * @param \Zend_Db_Adapter_Abstract $db
	 * @param string $lockName Lock name is server wide - should be prefixed by db name
	 */
	public function __construct(\Zend_Db_Adapter_Abstract $db, $lockName = '')
	{
		$this->_db = $db;
		$this->setLockName($lockName);
	}

	/**
	 * @param int $timeout
	 * @return bool
	 */
	public function lock($timeout = 0)
	{
		return (bool) $this->_db->fetchOne("SELECT GET_LOCK(?, ?)", array(
			$this->_prefixedLockName(),
			$timeout,
		));
	}

	public function isFree()
	{
		return (bool) $this->_db->fetchOne("SELECT IS_FREE_LOCK(?)", array($this->_prefixedLockName()));
	}

	public function unlock()
	{
		$this->_db->query("DO RELEASE_LOCK(?)", array($this->_prefixedLockName()));
	}

	protected function _prefixedLockName()
	{
		return $this->_dbName() . '.' . $this->_lockName;
	}

	protected function _dbName()
	{
		return $this->_db->fetchOne("SELECT DATABASE()");
	}

	public function getLockName()
	{
		return $this->_lockName;
	}

	public function setLockName($lockName)
	{
		$this->_lockName = $lockName;
	}

}