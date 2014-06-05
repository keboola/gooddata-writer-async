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
		$this->processJob('/filters', array(
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
		$this->processJob('/filters-user', array(
			"pid"       => $pid,
			"filters"   => array($filter['name']),
			"userEmail"    => $user['email']
		));
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
		$this->_createFilter($pid);

		// Check result
		$filterList = $this->configuration->getFilters();
		$this->assertCount(1, $filterList);

		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$gdFilters = $this->restApi->getFilters($pid);
		$gdFilter = $gdFilters[0];
		$this->assertEquals($gdFilter['link'], $filterList[0]['uri']);


		/**
		 * Assign filter to user
		 */
		$this->_assignFilterToUser($pid);

		// Check result
		$filtersUsers = $this->configuration->getFiltersUsers();
		$this->assertCount(1, $filtersUsers);


		/**
		 * Sync filters
		 */
		$this->processJob('/sync-filters', array(
			"pid"   => $pid,
		));

		// Check result
		$filterList = $this->configuration->getFilters();

		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$gdFilters = $this->restApi->getFilters($pid);
		$gdFilter = $gdFilters[0];
		$this->assertEquals($gdFilter['link'], $filterList[0]['uri']);


		/**
		 * Get filters
		 */
		$responseJson = $this->getWriterApi('/filters?writerId=' . $this->writerId);
		$this->assertArrayHasKey('filters', $responseJson, "Response for API call /filters should contain 'filters' key.");
		$this->assertNotEmpty($responseJson['filters'], "Response should not be empty.");

		// Get filters for user and pid
		$usersList = $this->configuration->getUsers();
		$user = $usersList[0];

		$responseJson = $this->getWriterApi('/filters?writerId=' . $this->writerId . '&userEmail=' . $user['email'] . '&pid=' . $pid);
		$this->assertArrayHasKey('filters', $responseJson, "Response for API call /filters should contain 'filters' key.");
		$this->assertNotEmpty($responseJson['filters'], "Response should not be empty.");


		/**
		 * Delete filter
		 */
		$filter = $filterList[0];
		$this->processJob('/filters?writerId=' . $this->writerId . '&uri=' . $filter['uri'], array(), 'DELETE');
		$this->assertFalse($this->configuration->getFilter($filter['name']), 'Writer should have no filter configured');
		$this->assertEmpty($this->configuration->getFiltersProjects(), 'Writer should have no filter-project relation configured');
		$this->assertEmpty($this->configuration->getFiltersUsers(), 'Writer should have no filter-user relation configured');
	}

}
