<?php
/**
 * Script access lock
 * Inspired by:
 *  - http://www.mysqlperformanceblog.com/2009/10/14/watch-out-for-your-cron-jobs/
 *  - http://www.phpdeveloper.org.uk/mysql-named-locks/
 *
 * @author Martin Halamicek <martin.halamicek@kebola.com>
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2011-12-27
 */

namespace Keboola\GoodDataWriter\Service;

class Lock
{

	/**
	 * @var \PDO
	 */
	protected $_db;
	protected $_lockName;


	/**
	 * @param \PDO $db
	 * @param string $lockName Lock name is server wide - should be prefixed by db name
	 */
	public function __construct(\PDO $db, $lockName = '')
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
		$sql = 'SELECT GET_LOCK(:name, :timeout)';
		$sth = $this->_db->prepare($sql, array(\PDO::ATTR_CURSOR => \PDO::CURSOR_FWDONLY));
		$sth->execute(array(':name' => $this->_prefixedLockName(), ':timeout' => $timeout));
		return $sth->fetchColumn();
	}

	public function isFree()
	{
		$sql = 'SELECT IS_FREE_LOCK(:name)';
		$sth = $this->_db->prepare($sql, array(\PDO::ATTR_CURSOR => \PDO::CURSOR_FWDONLY));
		$sth->execute(array(':name' => $this->_prefixedLockName()));
		return $sth->fetchColumn();
	}

	public function unlock()
	{
		$sql = 'DO RELEASE_LOCK(:name)';
		$sth = $this->_db->prepare($sql, array(\PDO::ATTR_CURSOR => \PDO::CURSOR_FWDONLY));
		$sth->execute(array(':name' => $this->_prefixedLockName()));
	}

	protected function _prefixedLockName()
	{
		return $this->_dbName() . '.' . $this->_lockName;
	}

	protected function _dbName()
	{
		$result = $this->_db->query('SELECT DATABASE()');
		return (string)$result->fetchColumn();
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