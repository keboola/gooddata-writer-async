<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */

namespace Keboola\GoodDataWriter\Tests\Controller;

use Keboola\GoodDataWriter\Writer\SharedConfig;
use Keboola\StorageApi\Table as StorageApiTable;
use Keboola\GoodDataWriter\GoodData\WebDav;

class RcTest extends AbstractControllerTest
{

	protected function setUp()
	{
		$this->httpClient = static::createClient();
		$container = $this->httpClient->getContainer();
		$this->storageApiToken = $container->getParameter('storage_api.test.token-rc');
		parent::setup();
	}


	public function testRcWriter()
	{
		$this->_prepareData();
		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		/**
		 * Upload whole project
		 */
		$this->_processJob('/gooddata-writer/upload-project');

		// Check existence of datasets in the project
		$data = $this->restApi->get('/gdc/md/' . $bucketAttributes['gd']['pid'] . '/data/sets');
		$this->assertArrayHasKey('dataSetsInfo', $data, "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo' key.");
		$this->assertArrayHasKey('sets', $data['dataSetsInfo'], "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo.sets' key.");
		$this->assertCount(4, $data['dataSetsInfo']['sets'], "Response for GoodData API call '/data/sets' should contain key 'dataSetsInfo.sets' with four values.");

		$dateFound = false;
		$dateTimeFound = false;
		$productsFound = false;
		$dateTimeDataLoad = false;
		$productsDataLoad = false;
		foreach ($data['dataSetsInfo']['sets'] as $d) {
			if ($d['meta']['identifier'] == 'dataset.time.productdate') {
				$dateTimeFound = true;
				if ($d['lastUpload']['dataUploadShort']['status'] == 'OK') {
					$dateTimeDataLoad = true;
				}
			}
			if ($d['meta']['identifier'] == 'productdate.dataset.dt') {
				$dateFound = true;
			}
			if ($d['meta']['identifier'] == 'dataset.products') {
				$productsFound = true;
				if ($d['lastUpload']['dataUploadShort']['status'] == 'OK') {
					$productsDataLoad = true;
				}
			}
		}
		$this->assertTrue($dateFound, "Date dimension has not been found in GoodData");
		$this->assertTrue($dateTimeFound, "Time dimension has not been found in GoodData");
		$this->assertTrue($productsFound, "Dataset 'Products' has not been found in GoodData");

		$this->assertTrue($dateTimeDataLoad, "Data to time dimension has not been loaded to GoodData");
		$this->assertTrue($productsDataLoad, "Data to dataset 'Products' has not been loaded to GoodData");


		/**
		 * Upload whole project once again
		 */
		$batchId = $this->_processJob('/gooddata-writer/upload-project');
		$response = $this->_getWriterApi('/gooddata-writer/batch?writerId=' . $this->writerId . '&batchId=' . $batchId);
		$this->assertArrayHasKey('batch', $response, "Response for writer call '/batch?batchId=' should contain key 'batch'.");
		$this->assertArrayHasKey('status', $response['batch'], "Response for writer call '/jobs?jobId=' should contain key 'batch.status'.");
		$this->assertEquals(SharedConfig::JOB_STATUS_SUCCESS, $response['batch']['status'], "Result of request /upload-project should be 'success'.");


		// Check validity of foreign keys (including time dimension during daylight saving switch values)
		$result = $this->restApi->validateProject($bucketAttributes['gd']['pid']);
		$this->assertEquals(0, $result['error_found'], 'Project validation should not contain errors but result is: ' . print_r($result, true));
		$this->assertEquals(0, $result['fatal_error_found'], 'Project validation should not contain errors but result is: ' . print_r($result, true));
	}

}
