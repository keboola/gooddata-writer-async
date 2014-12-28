<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2014-08-28
 */
namespace Keboola\GoodDataWriter\Tests;

use Keboola\GoodDataWriter\Service\Lock;
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;

class DbLockTest extends WebTestCase
{

	public function testLocks()
	{
		$container = static::createClient()->getContainer();
		$connectionParams = array(
			'dbname' => $container->getParameter('database_name'),
			'user' => $container->getParameter('database_user'),
			'password' => $container->getParameter('database_password'),
			'host' => $container->getParameter('database_host'),
			'port' => $container->getParameter('database_port'),
			'driver' => 'pdo_mysql',
			'charset' => 'utf8'
		);

		$db1 = \Doctrine\DBAL\DriverManager::getConnection($connectionParams);
		$db1->exec('SET wait_timeout = 31536000;');

		$db2 = \Doctrine\DBAL\DriverManager::getConnection($connectionParams);
		$db2->exec('SET wait_timeout = 31536000;');

		$lockName = uniqid();

		$lock1 = new Lock($db1, $lockName);
		$lock2 = new Lock($db2, $lockName);

		$this->assertTrue($lock1->lock(), 'Should successfully lock');
		$this->assertFalse($lock1->isFree(), 'Should tell lock not free');
		$this->assertFalse($lock2->isFree(), 'Should tell lock not free');
		$this->assertFalse($lock2->lock(), 'Should fail locking');
		$this->assertTrue($lock1->unlock(), 'Should successfully unlock');

		$this->assertTrue($lock2->lock(), 'Should successfully lock');
		$this->assertFalse($lock2->isFree(), 'Should tell lock not free');
		$this->assertFalse($lock1->isFree(), 'Should tell lock not free');
		$this->assertFalse($lock1->lock(), 'Should fail locking');
		$this->assertTrue($lock2->unlock(), 'Should successfully unlock');
	}

}
