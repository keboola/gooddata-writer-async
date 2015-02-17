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

use Doctrine\DBAL\Connection;

class Lock
{

    /**
     * @var Connection
     */
    private $db;
    private $lockName;


    public function __construct(Connection $db, $lockName = '')
    {
        $this->db = $db;
        $this->lockName = $db->getDatabase() . '.' . $lockName;
    }

    public function lock($timeout = 0)
    {
        return (bool)$this->db->fetchColumn('SELECT GET_LOCK(?, ?);', array($this->lockName, $timeout));
    }

    public function isFree()
    {
        return (bool)$this->db->fetchColumn('SELECT IS_FREE_LOCK(?);', array($this->lockName));
    }

    public function unlock()
    {
        return (bool)$this->db->executeQuery('DO RELEASE_LOCK(?);', array($this->lockName));
    }
}
