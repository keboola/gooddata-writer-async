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
	public function testMigration()
	{
		$configBucketId = 'sys.c-wr-gooddata-' . $this->writerId;

		$table = new StorageApiTable($this->storageApi, $configBucketId . '.filters', null, 'name');
		$table->setHeader(array('name', 'attribute', 'element', 'operator', 'uri'));
		$table->setFromArray(array(
			array('filter', 'out.c-main.products.id', 'Product 1', '=', '/gdc/md/pid/obj/1111')
		));
		$table->save();

		$table = new StorageApiTable($this->storageApi, $configBucketId . '.filters_projects', null, 'filterName');
		$table->setHeader(array('filterName', 'pid'));
		$table->setFromArray(array(
			array('filter', 'pid')
		));
		$table->save();

		$table = new StorageApiTable($this->storageApi, $configBucketId . '.filters_users', null, 'filterName');
		$table->setHeader(array('filterName', 'userEmail'));
		$table->setFromArray(array(
			array('filter', 'email')
		));
		$table->save();

		// Enforce migration
		$this->configuration->checkFiltersTable();

		$filtersTable = $this->storageApi->getTable($configBucketId . '.filters');
		$this->assertEquals(array('name', 'attribute', 'operator', 'value'), $filtersTable['columns'], 'Table filters has not been migrated successfully');
		$filtersTableData = StorageApiClient::parseCsv($this->storageApi->exportTable($configBucketId . '.filters'), true);
		$this->assertCount(1, $filtersTableData, 'Table filters has not been migrated successfully');
		$this->assertEquals(array('name' => 'filter', 'attribute' => 'out.c-main.products.id', 'operator' => '=',  'value' =>'Product 1'), current($filtersTableData), 'Table filters has not been migrated successfully');

		$filtersProjectsTable = $this->storageApi->getTable($configBucketId . '.filters_projects');
		$this->assertEquals(array('uri', 'filter', 'pid'), $filtersProjectsTable['columns'], 'Table filters_projects has not been migrated successfully');
		$filtersProjectsData = StorageApiClient::parseCsv($this->storageApi->exportTable($configBucketId . '.filters_projects'), true);
		$this->assertCount(1, $filtersProjectsData, 'Table filters_projects has not been migrated successfully');
		$this->assertEquals(array( 'uri' =>'/gdc/md/pid/obj/1111',  'filter' =>'filter',  'pid' =>'pid'), current($filtersProjectsData), 'Table filters_projects has not been migrated successfully');

		$filtersUsersTable = $this->storageApi->getTable($configBucketId . '.filters_users');
		$this->assertEquals(array('id', 'filter', 'email'), $filtersUsersTable['columns'], 'Table filters_users has not been migrated successfully');
		$filtersUsersData = StorageApiClient::parseCsv($this->storageApi->exportTable($configBucketId . '.filters_users'), true);
		$this->assertCount(1, $filtersUsersData, 'Table filters_users has not been migrated successfully');
		$this->assertEquals(array( 'id' =>sha1('filter.email'),  'filter' =>'filter',  'email' =>'email'), current($filtersUsersData), 'Table filters_users has not been migrated successfully');
	}

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
			'element' => 'Product 1'
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
	}

}
