<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
namespace Keboola\GoodDataWriter\Tests\Controller;

use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Table as StorageApiTable;

class FiltersTest extends AbstractControllerTest
{
	public function testFilters()
	{
		$bucketAttributes = $this->configuration->bucketAttributes();
		$pid = $bucketAttributes['gd']['pid'];

		// Upload data
		$this->prepareData();
		$this->processJob('/upload-project');


		/**
		 * Create filter
		 */
		$filterName = 'filter';
		$this->processJob('/filters', array(
			'pid' => $pid,
			'name' => $filterName,
			'attribute' => $this->dataBucketId . '.products.name',
			'value' => 'Product 1'
		));

		// Check result
		$filterList = $this->configuration->getFilters();
		$this->assertCount(1, $filterList, 'Configuration should contain one filter');

		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$gdFilters = $this->restApi->getFilters($pid);
		$this->assertCount(1, $gdFilters, 'Project should contain one created filter.');
		$gdFilter = $gdFilters[0];
		$this->assertEquals($gdFilter['title'], $filterName, 'Filter in project should have title ' . $filterName);


		/**
		 * Assign filter to user
		 */
		$usersList = $this->configuration->getUsers();
		$this->assertGreaterThan(0, $usersList, "Writer should have at least one user.");
		$user = $usersList[0];

		$filters = $this->configuration->getFilters();
		$this->assertGreaterThan(0, $filters, "Writer should have at least one filter.");
		$filter = $filters[0];

		$this->processJob('/filters-users', array(
			'pid' => $pid,
			'filters' => array($filter['name']),
			'email' => $user['email']
		));

		// Check configuration
		$this->assertCount(1, $this->configuration->getFiltersUsers(), 'List of users from getFiltersUsers() should contain the test user');
		$this->assertCount(1, $this->configuration->getFiltersForUser($user['email']), 'List of users from getFiltersForUser() should contain the test user');
		$this->assertCount(1, $this->configuration->getFiltersProjectsByPid($pid), 'List of users from getFiltersProjectsByPid() should contain the test user');



		/**
		 * Sync filters
		 * - Check filter's uri before and after the sync, it should be different
		 * - Create other filter via RestAPI, do sync and check that the filter does not exist anymore
		 */

		$this->restApi->createFilter('f2', 'attr.products.name', '=', 'Product 1', $pid);
		$gdFilters = $this->restApi->getFilters($pid);
		$this->assertCount(2, $gdFilters, 'Project should contain two created filter before the sync');

		$fp = $this->configuration->getFiltersProjectsByPid($pid);
		$oldFilterProject = current($fp);

		$this->processJob('/sync-filters', array(
			'pid' => $pid
		));

		$fp = $this->configuration->getFiltersProjectsByPid($pid);
		$newFilterProject = current($fp);
		$this->assertNotEquals($oldFilterProject['uri'], $newFilterProject['uri'], 'Filter should have different uri after the sync');
		$gdFilters = $this->restApi->getFilters($pid);
		$this->assertCount(1, $gdFilters, 'Project should contain one created filter after the sync');


		/**
		 * Configuration
		 */
		$usersList = $this->configuration->getUsers();
		$user = $usersList[0];

		$responseJson = $this->getWriterApi('/filters?writerId=' . $this->writerId);
		$this->assertArrayHasKey('filters', $responseJson, "Response for API call /filters should contain 'filters' key.");
		$this->assertNotEmpty($responseJson['filters'], "Response should not be empty.");
		$this->assertArrayHasKey('name', current($responseJson['filters']), "Row of /filters should contain 'name' key.");

		$responseJson = $this->getWriterApi(sprintf('/filters?writerId=%s&email=%s', $this->writerId, $user['email']));
		$this->assertArrayHasKey('filters', $responseJson, "Response for API call /filters should contain 'filters' key.");
		$this->assertNotEmpty($responseJson['filters'], "Response should not be empty.");

		$responseJson = $this->getWriterApi(sprintf('/filters?writerId=%s&pid=%s', $this->writerId, $pid));
		$this->assertArrayHasKey('filters', $responseJson, "Response for API call /filters should contain 'filters' key.");
		$this->assertNotEmpty($responseJson['filters'], "Response should not be empty.");


		$responseJson = $this->getWriterApi(sprintf('/filters-projects?writerId=%s', $this->writerId));
		$this->assertArrayHasKey('filters', $responseJson, "Response for API call /filters-projects should contain 'filters' key.");
		$this->assertNotEmpty($responseJson['filters'], "Response should not be empty.");
		$this->assertArrayHasKey('uri', current($responseJson['filters']), "Row of /filters-projects should contain 'pid' key.");

		$responseJson = $this->getWriterApi(sprintf('/filters-projects?writerId=%s&filter=%s', $this->writerId, $filterName));
		$this->assertArrayHasKey('filters', $responseJson, "Response for API call /filters-projects should contain 'filters' key.");
		$this->assertNotEmpty($responseJson['filters'], "Response should not be empty.");

		$responseJson = $this->getWriterApi(sprintf('/filters-projects?writerId=%s&pid=%s', $this->writerId, $pid));
		$this->assertArrayHasKey('filters', $responseJson, "Response for API call /filters-projects should contain 'filters' key.");
		$this->assertNotEmpty($responseJson['filters'], "Response should not be empty.");


		$responseJson = $this->getWriterApi(sprintf('/filters-users?writerId=%s', $this->writerId));
		$this->assertArrayHasKey('filters', $responseJson, "Response for API call /filters-users should contain 'filters' key.");
		$this->assertNotEmpty($responseJson['filters'], "Response should not be empty.");
		$this->assertArrayHasKey('id', current($responseJson['filters']), "Row of /filters-users should contain 'name' key.");

		$responseJson = $this->getWriterApi(sprintf('/filters-users?writerId=%s&filter=%s', $this->writerId, $filterName));
		$this->assertArrayHasKey('filters', $responseJson, "Response for API call /filters-users should contain 'filters' key.");
		$this->assertNotEmpty($responseJson['filters'], "Response should not be empty.");

		$responseJson = $this->getWriterApi(sprintf('/filters-users?writerId=%s&email=%s', $this->writerId, $user['email']));
		$this->assertArrayHasKey('filters', $responseJson, "Response for API call /filters-users should contain 'filters' key.");
		$this->assertNotEmpty($responseJson['filters'], "Response should not be empty.");


		/**
		 * Delete filter
		 */
		$filter = $filterList[0];
		$this->processJob('/filters?writerId=' . $this->writerId . '&name=' . $filter['name'], array(), 'DELETE');
		$this->assertFalse($this->configuration->getFilter($filter['name']), 'Writer should have no filter configured');
		$this->assertEmpty($this->configuration->getFiltersProjects(), 'Writer should have no filter-project relation configured');
		$this->assertEmpty($this->configuration->getFiltersUsers(), 'Writer should have no filter-user relation configured');


		/**
		 * Filter with OVER .. TO ..
		 */
		$table = new StorageApiTable($this->storageApi, $this->dataBucketId . '.users', null, 'id');
		$table->setHeader(array('id', 'name'));
		$table->setFromArray(array(array('u1', 'User 1'), array('u2', 'User 2')));
		$table->save();
		$this->configuration->updateDataSetDefinition($this->dataBucketId . '.users', 'name', 'Users');
		$this->configuration->updateDataSetDefinition($this->dataBucketId . '.users', 'export', '1');
		$this->configuration->updateColumnsDefinition($this->dataBucketId . '.users', array(
			array('name' => 'id', 'gdName' => 'Id', 'type' => 'CONNECTION_POINT'),
			array('name' => 'name', 'gdName' => 'Name', 'type' => 'ATTRIBUTE')
		));
		$this->processJob('/upload-table', array('tableId' => $this->dataBucketId . '.users'));

		$this->processJob('/filters', array(
			'pid' => $pid,
			'name' => 'Over-To Filter',
			'attribute' => $this->dataBucketId . '.users.name',
			'value' => 'User 1',
			'over' => $this->dataBucketId . '.users.id',
			'to' => $this->dataBucketId . '.products.id'
		));


		/**
		 * Filter with IN
		 */
		$batchId = $this->processJob('/filters', array(
			'pid' => $pid,
			'name' => 'Filter IN',
			'attribute' => $this->dataBucketId . '.products.name',
			'operator' => 'IN',
			'value' => 'Product 1'
		));
		$responseJson = $this->getWriterApi('/batch?writerId=' . $this->writerId . '&batchId=' . $batchId);
		$this->assertArrayHasKey('status', $responseJson, "Response for GoodData API call '/batch' should contain 'status' key.");
		$this->assertEquals('success', $responseJson['status'], "Batch '$batchId' should have status 'success'.");

		$batchId = $this->processJob('/filters', array(
			'pid' => $pid,
			'name' => 'Filter IN 2',
			'attribute' => $this->dataBucketId . '.products.name',
			'operator' => 'IN',
			'value' => array('Product 1', 'Product 2')
		));
		$responseJson = $this->getWriterApi('/batch?writerId=' . $this->writerId . '&batchId=' . $batchId);
		$this->assertArrayHasKey('status', $responseJson, "Response for GoodData API call '/batch' should contain 'status' key.");
		$this->assertEquals('success', $responseJson['status'], "Batch '$batchId' should have status 'success'.");
	}

}
