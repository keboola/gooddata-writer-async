<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */

namespace Keboola\GoodDataWriter\Tests\Controller;


use Keboola\GoodDataWriter\Writer\SharedConfig;

class JobsTest extends AbstractControllerTest
{

	public function testJobs()
	{
		$this->prepareData();


		/**
		 * Jobs info
		 */
		$responseJson = $this->postWriterApi('/upload-table', array(
			'writerId' => $this->writerId,
			'tableId' => $this->dataBucketId . '.categories'
		));

		$this->assertArrayHasKey('status', $responseJson, "Response for GoodData API call '/upload-table' should contain 'status' key.");
		$batchId = $responseJson['batch'];


		// Get all jobs
		$responseJson = $this->getWriterApi('/jobs?writerId=' . $this->writerId);
		$this->assertArrayHasKey('jobs', $responseJson, "Response for GoodData API call '/jobs' should contain 'jobs' key.");
		$this->assertCount(3, $responseJson['jobs'], "Response for GoodData API call '/jobs' should contain three jobs.");
		$uploadTableFound = false;
		foreach ($responseJson['jobs'] as $job) {
			$this->assertArrayHasKey('id', $job, "Response for GoodData API call '/jobs' should contain 'id' key in jobs list.");
			$this->assertArrayHasKey('command', $job, "Response for GoodData API call '/jobs' should contain 'id' key in jobs list.");
			if ($job['batchId'] == $batchId && $job['command'] == 'loadData') {
				$uploadTableFound = true;
			}
		}
		$this->assertTrue($uploadTableFound, "Response for GoodData API call '/jobs' should contain job of table upload.");


		// Get batch
		$responseJson = $this->postWriterApi('/upload-project', array('writerId' => $this->writerId));

		$this->assertArrayHasKey('batch', $responseJson, "Response for GoodData API call '/upload-project' should contain 'batch' key.");
		$batchId = $responseJson['batch'];

		$responseJson = $this->getWriterApi('/batch?writerId=' . $this->writerId . '&batchId=' . $batchId);
		$this->assertArrayHasKey('status', $responseJson, "Response for GoodData API call '/batch' should contain 'status' key.");
		$this->assertArrayHasKey('jobs', $responseJson, "Response for GoodData API call '/batch' should contain 'jobs' key.");
		$this->assertCount(6, $responseJson['jobs'], "Response for GoodData API call '/batch' should contain six jobs.");

		$uploadCategoriesFound = false;
		$uploadProductsFound = false;
		foreach ($responseJson['jobs'] as $job) {
			// Get job
			$this->assertArrayHasKey('command', $job, "Response for GoodData API call '/batch?batchId=' should contain 'command' key.");
			if ($job['command'] == 'loadData') {
				$this->assertArrayHasKey('parameters', $job, "Response for GoodData API call '/batch?batchId=' should contain 'parameters' key.");
				$this->assertArrayHasKey('tableId', $job['parameters'], "Response for GoodData API call '/batch?batchId=' should contain 'parameters.tableId' key.");
			}
			if ($job['command'] == 'loadData' && $job['parameters']['tableId'] == $this->dataBucketId . '.categories') {
				$uploadCategoriesFound = true;
			}
			if ($job['command'] == 'loadData' && $job['parameters']['tableId'] == $this->dataBucketId . '.products') {
				$uploadProductsFound = true;
			}
		}
		$this->assertTrue($uploadCategoriesFound, "Response for GoodData API call '/jobs' should contain job of table 'out.c-main.categories' upload.");
		$this->assertTrue($uploadProductsFound, "Response for GoodData API call '/jobs' should contain job of table 'out.c-main.products' upload.");



		/**
		 * Cancel jobs
		 */
		// Upload project
		$responseJson = $this->postWriterApi('/upload-project', array(
			'writerId' => $this->writerId
		));

		$this->assertArrayHasKey('batch', $responseJson, "Response for writer call '/upload-project' should contain 'batch' key.");


		// Get jobs of upload call
		$responseJson = $this->getWriterApi(sprintf('/batch?writerId=%s&batchId=%d', $this->writerId, $responseJson['batch']));

		$this->assertArrayHasKey('jobs', $responseJson, "Response for writer call '/batch' should contain 'jobs' key.");
		$jobs = $responseJson['jobs'];


		// Cancel jobs in queue
		$this->postWriterApi('/cancel-jobs', array(
			'writerId' => $this->writerId
		));


		// Check status of the jobs
		foreach ($jobs as $job) {
			$responseJson = $this->getWriterApi(sprintf('/jobs?writerId=%s&jobId=%d', $this->writerId, $job['id']));

			$this->assertArrayHasKey('status', $responseJson, "Response for writer call '/jobs' should contain 'status' key.");
			$this->assertEquals(SharedConfig::JOB_STATUS_CANCELLED, $responseJson['status'], "Response for writer call '/jobs' should have key 'status' with value 'cancelled'.");
		}
	}

}
