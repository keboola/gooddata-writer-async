<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
namespace Keboola\GoodDataWriter\Tests\Controller;

use Keboola\GoodDataWriter\Test\WriterTest,
	Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\StorageApi\Table as StorageApiTable;

class FiltersTest extends WriterTest
{

	public function testCreateFilter()
	{

		$this->_processJob('/gooddata-writer/filters', array(
			"pid"       => self::$configuration->bucketInfo['gd']['pid'],
			"name"      => "filter",
			"attribute" => "Name (Products)",
			"element"   => "Product 1"
		));

		// Check result
		$filterList = self::$configuration->getFilters();
		$this->assertCount(1, $filterList);
	}

	public function testAssignFilterToUser()
	{
		$usersList = self::$configuration->getUsers();
		$user = $usersList[0];

		$filters = self::$configuration->getFilters();
		$filter = $filters[0];

		// Create and process job
		$this->_processJob('/gooddata-writer/filters-user', array(
			"pid"       => self::$configuration->bucketInfo['gd']['pid'],
			"filters"   => array($filter['name']),
			"userEmail"    => $user['email']
		));

		// Check result
		$filtersUsers = self::$configuration->getFiltersUsers();
		$this->assertCount(1, $filtersUsers);
	}

	public function testSyncFilter()
	{
		$pid = self::$configuration->bucketInfo['gd']['pid'];

		// Create and process job
		$this->_processJob('/gooddata-writer/sync-filters', array(
			"pid"   => $pid,
		));

		// Check result
		$filterList = self::$configuration->getFilters();

		$gdFilters = self::$restApi->getFilters($pid);
		$gdFilter = $gdFilters[0];

		$this->assertEquals($gdFilter['link'], $filterList[0]['uri']);
	}

	public function testDeleteFilter()
	{
		$filters = self::$configuration->getFilters();
		$filter = $filters[0];

		// Create and process job
		$this->_processJob(
			'/gooddata-writer/filters?writerId=' . $this->writerId . '&uri=' . $filter['uri'] . '&dev=1',
			array(),
			'DELETE'
		);

		// Check result
		$filters = self::$configuration->getFilters();
		$this->assertCount(0, $filters);

		$filtersUsers = self::$configuration->getFiltersUsers();
		$this->assertCount(0, $filtersUsers);
	}

}
