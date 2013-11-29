<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
namespace Keboola\GoodDataWriter\Tests\Controller;

use Aws\Common\Facade\ElasticBeanstalk;
use Keboola\Csv\CsvFile;

class DatasetsTest extends AbstractControllerTest
{

	/*public function testXml()
	{
		//@TODO
	}*/

	public function testUploadProject()
	{
		$this->_prepareData();

		$this->_processJob('/gooddata-writer/upload-project');

		// Check existence of datasets in the project
		$bucketAttributes = self::$configuration->bucketAttributes();
		self::$restApi->setCredentials($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$data = self::$restApi->get('/gdc/md/' . $bucketAttributes['gd']['pid'] . '/data/sets');
		$this->assertArrayHasKey('dataSetsInfo', $data, "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo' key.");
		$this->assertArrayHasKey('sets', $data['dataSetsInfo'], "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo.sets' key.");
		$this->assertCount(4, $data['dataSetsInfo']['sets'], "Response for GoodData API call '/data/sets' should contain key 'dataSetsInfo.sets' with four values.");

		$dateFound = false;
		$dateTimeFound = false;
		$categoriesFound = false;
		$productsFound = false;
		$dateTimeDataLoad = false;
		$categoriesDataLoad = false;
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
			if ($d['meta']['identifier'] == 'dataset.categories') {
				$categoriesFound = true;
				if ($d['lastUpload']['dataUploadShort']['status'] == 'OK') {
					$categoriesDataLoad = true;
				}
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
		$this->assertTrue($categoriesFound, "Dataset 'Categories' has not been found in GoodData");
		$this->assertTrue($productsFound, "Dataset 'Products' has not been found in GoodData");

		$this->assertTrue($dateTimeDataLoad, "Data to time dimension has not been loaded to GoodData");
		$this->assertTrue($categoriesDataLoad, "Data to dataset 'Categories' has not been loaded to GoodData");
		$this->assertTrue($productsDataLoad, "Data to dataset 'Products' has not been loaded to GoodData");


		// Run once again
		$batchId = $this->_processJob('/gooddata-writer/upload-project');
		$response = $this->_getWriterApi('/gooddata-writer/batch?writerId=' . $this->writerId . '&batchId=' . $batchId);
		$this->assertArrayHasKey('batch', $response, "Response for writer call '/batch?batchId=' should contain key 'batch'.");
		$this->assertArrayHasKey('status', $response['batch'], "Response for writer call '/jobs?jobId=' should contain key 'batch.status'.");
		$this->assertEquals('success', $response['batch']['status'], "Result of second /upload-project should be 'success'.");
	}


	public function testUploadTable()
	{
		$this->_prepareData();

		$jobId = $this->_processJob('/gooddata-writer/upload-table', array('tableId' => $this->dataBucketId . '.categories'));

		// Check existence of datasets in the project
		$bucketAttributes = self::$configuration->bucketAttributes();
		self::$restApi->setCredentials($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$data = self::$restApi->get('/gdc/md/' . $bucketAttributes['gd']['pid'] . '/data/sets');
		$this->assertArrayHasKey('dataSetsInfo', $data, "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo' key.");
		$this->assertArrayHasKey('sets', $data['dataSetsInfo'], "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo.sets' key.");
		$this->assertCount(1, $data['dataSetsInfo']['sets'], "Response for GoodData API call '/data/sets' should contain key 'dataSetsInfo.sets' with one value.");
		$this->assertEquals('dataset.categories', $data['dataSetsInfo']['sets'][0]['meta']['identifier'], "GoodData project should contain dataset 'Categories'.");

		// Check csv of main project if contains all rows
		$csvFile = sprintf('%s/%s/%s/data.csv', self::$mainConfig['tmp_path'], $jobId, $bucketAttributes['gd']['pid']);
		$this->assertTrue(file_exists($csvFile), sprintf("Data csv file '%s' should exist.", $csvFile));
		$csv = new CsvFile($csvFile);
		$rowsNumber = 0;
		foreach ($csv as $row) {
			$rowsNumber++;
		}
		$this->assertEquals(3, $rowsNumber, "Csv of main project should contain two rows with header.");
	}


	/*public function testGetModel()
	{
		//@TODO exit() in GoodDataWriter::getModel() stops test execution
		self::$client->request('GET', sprintf('/gooddata-writer/model?writerId=%s', $this->writerId));
		$response = self::$client->getResponse();
		$responseJson = json_decode($response->getContent(), true);

		$this->assertArrayHasKey('nodes', $responseJson);
		$this->assertArrayHasKey('links', $responseJson);
		$this->assertCount(2, $responseJson['nodes']);
		$this->assertCount(1, $responseJson['links']);
	}*/


	public function testGetTables()
	{
		$this->_prepareData();

		$responseJson = $this->_getWriterApi('/gooddata-writer/tables?writerId=' . $this->writerId);

		$this->assertArrayHasKey('tables', $responseJson, "Response for writer call '/tables' should contain 'tables' key.");

		// Filter out tables not belonging to this test
		$tables = array();
		foreach ($responseJson['tables'] as $t) {
			if ($t['bucket'] == $this->dataBucketId) {
				$tables[] = $t;
			}
		}

		$this->assertCount(2, $tables, "Response for writer call '/tables' should contain two configured tables.");
		foreach ($tables as $table) {
			$this->assertArrayHasKey('gdName', $table, sprintf("Table '%s' should have 'gdName' attribute.", $table['id']));
			$this->assertTrue(in_array($table['gdName'], array('Products', 'Categories')), sprintf("Table '%s' does not belong to configured tables.", $table['id']));
			$this->assertArrayHasKey('lastExportDate', $table, sprintf("Table '%s' should have 'lastExportDate' attribute.", $table['id']));
		}
	}

	public function testGetDataSetsWithConnectionPoint()
	{
		$this->_prepareData();

		$responseJson = $this->_getWriterApi('/gooddata-writer/tables?writerId=' . $this->writerId . '&referenceable');

		$this->assertArrayHasKey('tables', $responseJson, "Response for writer call '/tables?referenceable' should contain 'tables' key.");

		$this->assertCount(2, $responseJson['tables'], "Response for writer call '/tables?referenceable' should contain two configured tables.");
		foreach ($responseJson['tables'] as $table) {
			$this->assertArrayHasKey('referenceable', $table, "There should be 'referenceable' flag of each table.");
			$this->assertEquals(1, $table['referenceable'], "Both tables should be referenceable");
		}
	}

	public function testGetSpecificTable()
	{
		$this->_prepareData();

		$responseJson = $this->_getWriterApi('/gooddata-writer/tables?writerId=' . $this->writerId . '&tableId=' . $this->dataBucketId . '.products');

		$this->assertArrayHasKey('table', $responseJson, "Response for writer call '/tables?tableId=' should contain 'table' key.");
		$this->assertArrayHasKey('tableId', $responseJson['table'], "Response for writer call '/tables?tableId=' should contain 'table.tableId' key.");
		$this->assertArrayHasKey('name', $responseJson['table'], "Response for writer call '/tables?tableId=' should contain 'table.name' key.");
		$this->assertArrayHasKey('columns', $responseJson['table'], "Response for writer call '/tables?tableId=' should contain 'table.columns' key.");
		$this->assertEquals($this->dataBucketId . '.products', $responseJson['table']['tableId'], "Response for writer call '/tables?tableId=' should contain 'table.tableId' key with value of data bucket Products.");
		$this->assertCount(5, $responseJson['table']['columns'], "Response for writer call '/tables?tableId=' should contain 'table.columns' key with five columns.");
	}


	public function testPostTables()
	{
		$this->_prepareData();

		$tableId = $this->dataBucketId . '.products';
		$testName = uniqid('test-name');

		// Change gdName of table
		$this->_postWriterApi('/gooddata-writer/tables', array(
			'writerId' => $this->writerId,
			'tableId' => $tableId,
			'name' => $testName
		));

		// Check if GD name was changed
		$responseJson = $this->_getWriterApi('/gooddata-writer/tables?writerId=' . $this->writerId);
		$this->assertArrayHasKey('tables', $responseJson, "Response for writer call '/tables' should contain 'tables' key.");

		$testResult = false;
		$lastChangeDate = null;
		foreach ($responseJson['tables'] as $t) {
			if ($t['id'] == $tableId) {
				$this->assertArrayHasKey('name', $t);
				if ($t['name'] == $testName) {
					$testResult = true;
				}
				$lastChangeDate = $t['lastChangeDate'];
			}
		}
		$this->assertTrue($testResult, "Changed name was not found in configuration.");
		$this->assertNotEmpty($lastChangeDate, "Change of name did not set 'lastChangeDate' attribute");


		// Change gdName again and check if lastChangeDate changed
		$this->_postWriterApi('/gooddata-writer/tables', array(
			'writerId' => $this->writerId,
			'tableId' => $tableId,
			'name' => $testName . '2'
		));

		$responseJson = $this->_getWriterApi('/gooddata-writer/tables?writerId=' . $this->writerId);
		$lastChangeDateAfterUpdate = null;
		foreach ($responseJson['tables'] as $t) {
			if ($t['id'] != $tableId) {
				continue;
			}
			$lastChangeDateAfterUpdate = $t['lastChangeDate'];
		}

		$this->assertNotEquals($lastChangeDate, $lastChangeDateAfterUpdate, 'Last change date should be changed after update');
	}


	public function testResetExport()
	{
		$this->_prepareData();
		$tableId = $this->dataBucketId . '.categories';

		$this->_processJob('/gooddata-writer/upload-table', array('tableId' => $tableId));

		$responseJson = $this->_getWriterApi('/gooddata-writer/tables?writerId=' . $this->writerId . '&tableId=' . $tableId);
		$this->assertArrayHasKey('lastExportDate', $responseJson['table'], "Exported table should contain 'lastExportDate' attribute.");

		$this->_postWriterApi('/gooddata-writer/reset-export', array('writerId' => $this->writerId));

		$responseJson = $this->_getWriterApi('/gooddata-writer/tables?writerId=' . $this->writerId . '&tableId=' . $tableId);
		$this->assertEquals(0, $responseJson['table']['lastExportDate'], "Reset table should contain empty 'lastExportDate' attribute.");
	}


	public function testRemoveColumn()
	{
		$this->_prepareData();
		$tableId = $this->dataBucketId . '.products';
		$nowTime = date('c');

		// Remove column and test if lastChangeDate changed
		self::$storageApi->deleteTableColumn($tableId, 'price');

		$responseJson = $this->_getWriterApi('/gooddata-writer/tables?writerId=' . $this->writerId . '&tableId=' . $tableId);
		$this->assertArrayHasKey('table', $responseJson, "Response for writer call '/tables&tableId=' should contain 'table' key.");

		$this->assertArrayHasKey('lastChangeDate', $responseJson['table'], "Response for writer call '/tables&tableId=' should contain 'table.lastChangeDate' key.");
		$this->assertGreaterThan($nowTime, $responseJson['table']['lastChangeDate'], "Response for writer call '/tables&tableId=' should have 'table.lastChangeDate' updated.");
	}

	public function testDateDimensions()
	{
		//@TODO get, delete, post
		$this->_prepareData();

		// Create dimension
		$this->_postWriterApi('/gooddata-writer/date-dimensions', array(
			'writerId' => $this->writerId,
			'name' => 'TestDate'
		));

		// Get dimensions
		$responseJson = $this->_getWriterApi('/gooddata-writer/date-dimensions?writerId=' . $this->writerId . '&usage');
		$this->assertArrayHasKey('dimensions', $responseJson, "Response for writer call '/date-dimensions' should contain 'dimensions' key.");
		$this->assertCount(2, $responseJson['dimensions'], "Response for writer call '/date-dimensions' should contain two dimensions.");
		$this->assertArrayHasKey('TestDate', $responseJson['dimensions'], "Response for writer call '/date-dimensions' should contain dimension 'TestDate'.");
		$this->assertArrayHasKey('ProductDate', $responseJson['dimensions'], "Response for writer call '/date-dimensions' should contain dimension 'ProductDate'.");
		$this->assertArrayHasKey('usedIn', $responseJson['dimensions']['ProductDate'], "Response for writer call '/date-dimensions' should contain key 'usedIn' for dimension 'ProductDate'.");
		$this->assertCount(1, $responseJson['dimensions']['ProductDate']['usedIn'], "Response for writer call '/date-dimensions' should contain usage of dimension 'ProductDate'.");
		$this->assertEquals($this->dataBucketId . '.products', $responseJson['dimensions']['ProductDate']['usedIn'][0], "Response for writer call '/date-dimensions' should contain usage of dimension 'ProductDate' in dataset 'Products'.");

		// Drop dimension
		$this->_callWriterApi('/gooddata-writer/date-dimensions?writerId=' . $this->writerId . '&name=TestDate', 'DELETE');
		$responseJson = $this->_getWriterApi('/gooddata-writer/date-dimensions?writerId=' . $this->writerId);
		$this->assertCount(1, $responseJson['dimensions'], "Response for writer call '/date-dimensions' should contain one dimension.");
		$this->assertArrayNotHasKey('TestDate', $responseJson['dimensions'], "Response for writer call '/date-dimensions' should not contain dimension 'TestDate'.");
	}


}
