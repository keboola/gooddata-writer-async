<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */

namespace Keboola\GoodDataWriter\Tests\Controller;


class JobsTest extends AbstractControllerTest
{

	public function testJobs()
	{
		$this->_prepareData();


		/**
		 * Jobs info
		 */
		$responseJson = $this->_postWriterApi('/gooddata-writer/upload-table', array(
			'writerId' => $this->writerId,
			'tableId' => $this->dataBucketId . '.categories'
		));

		$this->assertArrayHasKey('job', $responseJson, "Response for GoodData API call '/upload-table' should contain 'job' key.");
		$jobId = $responseJson['job'];


		// Get all jobs
		$responseJson = $this->_getWriterApi('/gooddata-writer/jobs?writerId=' . $this->writerId);
		$this->assertArrayHasKey('jobs', $responseJson, "Response for GoodData API call '/jobs' should contain 'jobs' key.");
		$this->assertCount(2, $responseJson['jobs'], "Response for GoodData API call '/jobs' should contain two jobs.");
		$uploadTableFound = false;
		foreach ($responseJson['jobs'] as $job) {
			$this->assertArrayHasKey('id', $job, "Response for GoodData API call '/jobs' should contain 'id' key in jobs list.");
			$this->assertArrayHasKey('command', $job, "Response for GoodData API call '/jobs' should contain 'id' key in jobs list.");
			if ($job['id'] == $jobId && $job['command'] == 'uploadTable') {
				$uploadTableFound = true;
			}
		}
		$this->assertTrue($uploadTableFound, "Response for GoodData API call '/jobs' should contain job of table upload.");


		// Get batch
		$responseJson = $this->_postWriterApi('/gooddata-writer/upload-project', array('writerId' => $this->writerId));

		$this->assertArrayHasKey('batch', $responseJson, "Response for GoodData API call '/upload-project' should contain 'batch' key.");
		$batchId = $responseJson['batch'];

		$responseJson = $this->_getWriterApi('/gooddata-writer/batch?writerId=' . $this->writerId . '&batchId=' . $batchId);
		$this->assertArrayHasKey('batch', $responseJson, "Response for GoodData API call '/batch' should contain 'batch' key.");
		$this->assertArrayHasKey('jobs', $responseJson['batch'], "Response for GoodData API call '/batch' should contain 'batch.jobs' key.");
		$this->assertCount(4, $responseJson['batch']['jobs'], "Response for GoodData API call '/batch' should contain four jobs.");

		$uploadCategoriesFound = false;
		$uploadProductsFound = false;
		foreach ($responseJson['batch']['jobs'] as $jobId) {
			// Get job
			$jobResponse = $this->_getWriterApi('/gooddata-writer/jobs?writerId=' . $this->writerId . '&jobId=' . $jobId);
			$this->assertArrayHasKey('job', $jobResponse, "Response for GoodData API call '/jobs?jobId=' should contain 'job' key.");
			$this->assertArrayHasKey('command', $jobResponse['job'], "Response for GoodData API call '/jobs?jobId=' should contain 'job.command' key.");
			if ($jobResponse['job']['command'] == 'uploadTable') {
				$this->assertArrayHasKey('parameters', $jobResponse['job'], "Response for GoodData API call '/jobs?jobId=' should contain 'job.parameters' key.");
				$this->assertArrayHasKey('tableId', $jobResponse['job']['parameters'], "Response for GoodData API call '/jobs?jobId=' should contain 'job.parameters.tableId' key.");
			}
			if ($jobResponse['job']['command'] == 'uploadTable' && $jobResponse['job']['parameters']['tableId'] == $this->dataBucketId . '.categories') {
				$uploadCategoriesFound = true;
			}
			if ($jobResponse['job']['command'] == 'uploadTable' && $jobResponse['job']['parameters']['tableId'] == $this->dataBucketId . '.products') {
				$uploadProductsFound = true;
			}
		}
		$this->assertTrue($uploadCategoriesFound, "Response for GoodData API call '/jobs' should contain job of table 'out.c-main.categories' upload.");
		$this->assertTrue($uploadProductsFound, "Response for GoodData API call '/jobs' should contain job of table 'out.c-main.products' upload.");



		/**
		 * Cancel jobs
		 */
		// Upload project
		$responseJson = $this->_postWriterApi('/gooddata-writer/upload-project', array(
			'writerId' => $this->writerId
		));

		$this->assertArrayHasKey('batch', $responseJson, "Response for writer call '/upload-project' should contain 'batch' key.");


		// Get jobs of upload call
		$responseJson = $this->_getWriterApi(sprintf('/gooddata-writer/batch?writerId=%s&batchId=%d', $this->writerId, $responseJson['batch']));

		$this->assertArrayHasKey('batch', $responseJson, "Response for writer call '/batch' should contain 'batch' key.");
		$this->assertArrayHasKey('jobs', $responseJson['batch'], "Response for writer call '/batch' should contain 'batch.jobs' key.");
		$jobs = $responseJson['batch']['jobs'];


		// Cancel jobs in queue
		$this->_postWriterApi('/gooddata-writer/cancel-jobs', array(
			'writerId' => $this->writerId
		));


		// Check status of the jobs
		foreach ($jobs as $jobId) {
			$responseJson = $this->_getWriterApi(sprintf('/gooddata-writer/jobs?writerId=%s&jobId=%d', $this->writerId, $jobId));

			$this->assertArrayHasKey('status', $responseJson, "Response for writer call '/jobs' should contain 'status' key.");
			$this->assertArrayHasKey('job', $responseJson, "Response for writer call '/jobs' should contain 'job' key.");
			$this->assertArrayHasKey('status', $responseJson['job'], "Response for writer call '/jobs' should contain 'job.status' key.");
			$this->assertEquals('cancelled', $responseJson['job']['status'], "Response for writer call '/jobs' should have key 'job.status' with value 'cancelled'.");
		}
	}

}
