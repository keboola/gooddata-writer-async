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
	protected function _createFilter($pid)
	{
		$this->_processJob('/gooddata-writer/filters', array(
			"pid"       => $pid,
			"name"      => "filter",
			"attribute" => $this->dataBucketId . '.' . 'products' . '.' . 'name',
			"element"   => "Product 1"
		));
	}

	protected function _assignFilterToUser($pid)
	{
		$usersList = self::$configuration->getUsers();
		$user = $usersList[0];

		$filters = self::$configuration->getFilters();
		$filter = $filters[0];

		// Create and process job
		$this->_processJob('/gooddata-writer/filters-user', array(
			"pid"       => $pid,
			"filters"   => array($filter['name']),
			"userEmail"    => $user['email']
		));
	}

	public function testCreateFilter()
	{
		$pid = self::$configuration->bucketInfo['gd']['pid'];

		// Upload data
		$this->_prepareData();
		$this->_processJob('/gooddata-writer/upload-project');

		$this->_createFilter($pid);

		// Check result
		$filterList = self::$configuration->getFilters();
		$this->assertCount(1, $filterList);

		self::$restApi->login(
			self::$configuration->bucketInfo['gd']['username'],
			self::$configuration->bucketInfo['gd']['password']
		);
		$gdFilters = self::$restApi->getFilters($pid);
		$gdFilter = $gdFilters[0];
		$this->assertEquals($gdFilter['link'], $filterList[0]['uri']);
	}

	public function testAssignFilterToUser()
	{
		$pid = self::$configuration->bucketInfo['gd']['pid'];

		// Upload data
		$this->_prepareData();
		$this->_processJob('/gooddata-writer/upload-project');

		$this->_createFilter($pid);
		$this->_assignFilterToUser($pid);

		// Check result
		$filtersUsers = self::$configuration->getFiltersUsers();
		$this->assertCount(1, $filtersUsers);
	}

	public function testSyncFilter()
	{
		$pid = self::$configuration->bucketInfo['gd']['pid'];

		// Upload data
		$this->_prepareData();
		$this->_processJob('/gooddata-writer/upload-project');

		$this->_createFilter($pid);
		$this->_assignFilterToUser($pid);

		// Create and process job
		$this->_processJob('/gooddata-writer/sync-filters', array(
			"pid"   => $pid,
		));

		// Check result
		$filterList = self::$configuration->getFilters();

		self::$restApi->login(
			self::$configuration->bucketInfo['gd']['username'],
			self::$configuration->bucketInfo['gd']['password']
		);
		$gdFilters = self::$restApi->getFilters($pid);
		$gdFilter = $gdFilters[0];
		$this->assertEquals($gdFilter['link'], $filterList[0]['uri']);
	}

	public function testDeleteFilter()
	{
		$pid = self::$configuration->bucketInfo['gd']['pid'];

		// Upload data
		$this->_prepareData();
		$this->_processJob('/gooddata-writer/upload-project');

		$this->_createFilter($pid);
		$this->_assignFilterToUser($pid);

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
