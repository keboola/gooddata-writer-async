<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
namespace Keboola\GoodDataWriter\Tests\Controller;

class FiltersTest extends AbstractControllerTest
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
		$usersList = $this->configuration->getUsers();
		$this->assertGreaterThan(0, $usersList, "Writer should have at least one user.");
		$user = $usersList[0];

		$filters = $this->configuration->getFilters();
		$this->assertGreaterThan(0, $filters, "Writer should have at least one filter.");
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
		$bucketAttributes = $this->configuration->bucketAttributes();
		$pid = $bucketAttributes['gd']['pid'];

		// Upload data
		$this->_prepareData();
		$this->_processJob('/gooddata-writer/upload-project');

		$this->_createFilter($pid);

		// Check result
		$filterList = $this->configuration->getFilters();
		$this->assertCount(1, $filterList);

		$this->restApi->setCredentials(
			$bucketAttributes['gd']['username'],
			$bucketAttributes['gd']['password']
		);
		$gdFilters = $this->restApi->getFilters($pid);
		$gdFilter = $gdFilters[0];
		$this->assertEquals($gdFilter['link'], $filterList[0]['uri']);
	}

	public function testAssignFilterToUser()
	{
		$bucketAttributes = $this->configuration->bucketAttributes();
		$pid = $bucketAttributes['gd']['pid'];

		// Upload data
		$this->_prepareData();
		$this->_processJob('/gooddata-writer/upload-project');

		$this->_createFilter($pid);
		$this->_assignFilterToUser($pid);

		// Check result
		$filtersUsers = $this->configuration->getFiltersUsers();
		$this->assertCount(1, $filtersUsers);
	}

	public function testSyncFilter()
	{
		$bucketAttributes = $this->configuration->bucketAttributes();
		$pid = $bucketAttributes['gd']['pid'];

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
		$filterList = $this->configuration->getFilters();

		$this->restApi->setCredentials(
			$bucketAttributes['gd']['username'],
			$bucketAttributes['gd']['password']
		);
		$gdFilters = $this->restApi->getFilters($pid);
		$gdFilter = $gdFilters[0];
		$this->assertEquals($gdFilter['link'], $filterList[0]['uri']);
	}

	public function testDeleteFilter()
	{
		$bucketAttributes = $this->configuration->bucketAttributes();
		$pid = $bucketAttributes['gd']['pid'];

		// Upload data
		$this->_prepareData();
		$this->_processJob('/gooddata-writer/upload-project');

		$this->_createFilter($pid);
		$this->_assignFilterToUser($pid);

		$filters = $this->configuration->getFilters();
		$filter = $filters[0];

		// Create and process job
		$this->_processJob(
			'/gooddata-writer/filters?writerId=' . $this->writerId . '&uri=' . $filter['uri'] . '&dev=1',
			array(),
			'DELETE'
		);

		// Check result
		$filters = $this->configuration->getFilters();
		$this->assertCount(0, $filters);

		$filtersUsers = $this->configuration->getFiltersUsers();
		$this->assertCount(0, $filtersUsers);
	}

	public function testGetFilters()
	{
		$bucketAttributes = $this->configuration->bucketAttributes();
		$pid = $bucketAttributes['gd']['pid'];

		// Upload data
		$this->_prepareData();
		$this->_processJob('/gooddata-writer/upload-project');

		$this->_createFilter($pid);
		$this->_assignFilterToUser($pid);

		$responseJson = $this->_getWriterApi('/gooddata-writer/filters?writerId=' . $this->writerId);
		$this->assertArrayHasKey('filters', $responseJson, "Response for API call /filters should contain 'filters' key.");
		$this->assertNotEmpty($responseJson['filters'], "Response should not be empty.");

		// Get filters for user and pid
		$usersList = $this->configuration->getUsers();
		$user = $usersList[0];

		$responseJson = $this->_getWriterApi('/gooddata-writer/filters?writerId=' . $this->writerId . '&userEmail=' . $user['email'] . '&pid=' . $pid);
		$this->assertArrayHasKey('filters', $responseJson, "Response for API call /filters should contain 'filters' key.");
		$this->assertNotEmpty($responseJson['filters'], "Response should not be empty.");
	}

}
