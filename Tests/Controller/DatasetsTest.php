<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
namespace Keboola\GoodDataWriter\Tests\Controller;

use Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\StorageApi\Table as StorageApiTable;

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
		self::$restApi->setCredentials(self::$configuration->bucketInfo['gd']['username'], self::$configuration->bucketInfo['gd']['password']);
		$data = self::$restApi->get('/gdc/md/' . self::$configuration->bucketInfo['gd']['pid'] . '/data/sets');
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
		$this->assertTrue($categoriesFound, "Dataset 'Categories' has not been found in GoodData");
		$this->assertTrue($productsFound, "Dataset 'Products' has not been found in GoodData");
	}


	public function testUploadTable()
	{
		$this->_prepareData();

		$this->_processJob('/gooddata-writer/upload-table', array('tableId' => $this->dataBucketId . '.categories'));

		// Check existence of datasets in the project
		self::$restApi->setCredentials(self::$configuration->bucketInfo['gd']['username'], self::$configuration->bucketInfo['gd']['password']);
		$data = self::$restApi->get('/gdc/md/' . self::$configuration->bucketInfo['gd']['pid'] . '/data/sets');
		$this->assertArrayHasKey('dataSetsInfo', $data, "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo' key.");
		$this->assertArrayHasKey('sets', $data['dataSetsInfo'], "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo.sets' key.");
		$this->assertCount(1, $data['dataSetsInfo']['sets'], "Response for GoodData API call '/data/sets' should contain key 'dataSetsInfo.sets' with one value.");
		$this->assertEquals('dataset.categories', $data['dataSetsInfo']['sets'][0]['meta']['identifier'], "GoodData project should contain dataset 'Categories'.");
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

	public function testGetReferenceableTables()
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
		$this->assertArrayHasKey('gdName', $responseJson['table'], "Response for writer call '/tables?tableId=' should contain 'table.gdName' key.");
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
			'gdName' => $testName
		));

		// Check if GD name was changed
		$responseJson = $this->_getWriterApi('/gooddata-writer/tables?writerId=' . $this->writerId);
		$this->assertArrayHasKey('tables', $responseJson, "Response for writer call '/tables' should contain 'tables' key.");

		$testResult = false;
		$lastChangeDate = null;
		foreach ($responseJson['tables'] as $t) {
			if ($t['id'] == $tableId) {
				$this->assertArrayHasKey('gdName', $t);
				if ($t['gdName'] == $testName) {
					$testResult = true;
				}
				$lastChangeDate = $t['lastChangeDate'];
			}
		}
		$this->assertTrue($testResult, "Changed gdName was not found in configuration.");
		$this->assertNotEmpty($lastChangeDate, "Change of gdName did not set 'lastChangeDate' attribute");


		// Change gdName again and check if lastChangeDate changed
		$this->_postWriterApi('/gooddata-writer/tables', array(
			'writerId' => $this->writerId,
			'tableId' => $tableId,
			'gdName' => $testName . '2'
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

		$this->_processJob('/gooddata-writer/upload-table', array('tableId' => $this->dataBucketId . '.categories'));

		$responseJson = $this->_getWriterApi('/gooddata-writer/tables?writerId=' . $this->writerId . '&tableId=' . $this->dataBucketId . '.categories');
		$this->assertArrayHasKey('lastExportDate', $responseJson['table'], "Exported table should contain 'lastExportDate' attribute.");

		$this->_postWriterApi('/gooddata-writer/reset-export', array('writerId' => $this->writerId));

		$responseJson = $this->_getWriterApi('/gooddata-writer/tables?writerId=' . $this->writerId . '&tableId=' . $this->dataBucketId . '.categories');
		$this->assertEquals('', $responseJson['table']['lastExportDate'], "Reset table should contain empty 'lastExportDate' attribute.");
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


		// Update dimension
		$date = date('c');
		$this->_postWriterApi('/gooddata-writer/date-dimensions', array(
			'writerId' => $this->writerId,
			'name' => 'ProductDate',
			'lastExportDate' => $date
		));

		$responseJson = $this->_getWriterApi('/gooddata-writer/date-dimensions?writerId=' . $this->writerId);
		$this->assertArrayHasKey('lastExportDate', $responseJson['dimensions']['ProductDate'], "Response for writer call '/date-dimensions' should contain key 'lastExportDate' for dimension 'ProductDate'.");
		$this->assertEquals($date, $responseJson['dimensions']['ProductDate']['lastExportDate'], "Response for writer call '/date-dimensions' should contain key 'lastExportDate' for dimension 'ProductDate' with proper value.");


		// Drop dimension
		$this->_callWriterApi('/gooddata-writer/date-dimensions?writerId=' . $this->writerId . '&name=TestDate', 'DELETE');
		$responseJson = $this->_getWriterApi('/gooddata-writer/date-dimensions?writerId=' . $this->writerId);
		$this->assertCount(1, $responseJson['dimensions'], "Response for writer call '/date-dimensions' should contain one dimension.");
		$this->assertArrayNotHasKey('TestDate', $responseJson['dimensions'], "Response for writer call '/date-dimensions' should not contain dimension 'TestDate'.");
	}


	public function testUploadFilteredTable()
	{
		self::$storageApi->createBucket($this->dataBucketName, 'out', 'Writer Test');
		$filteredTableName = 'filteredTable';
		$notFilteredTableName = 'notFilteredTable';


		// Clone project
		$this->_processJob('/gooddata-writer/projects', array());
		$clonedPid = null;
		foreach (self::$configuration->getProjects() as $p) if (empty($p['main'])) {
			$clonedPid = $p['pid'];
		}
		$this->assertNotEmpty($clonedPid, "Configuration should contain a cloned project.");


		// Prepare data
		$table = new StorageApiTable(self::$storageApi, $this->dataBucketId . '.' . $filteredTableName, null, 'id');
		$table->setHeader(array('id', 'name', 'pid'));
		$table->addIndex('pid');
		$table->setFromArray(array(
			array('u1', 'User 1', 'x'),
			array('u2', 'User 2', $clonedPid)
		));
		$table->save();

		$table = new StorageApiTable(self::$storageApi, $this->dataBucketId . '.' . $notFilteredTableName, null, 'id');
		$table->setHeader(array('id', 'name'));
		$table->setFromArray(array(
			array('x1', 'X 1'),
			array('x2', 'X 2')
		));
		$table->save();


		// Prepare configuration
		self::$configuration->setBucketAttribute('filterColumn', 'pid');

		$table = new StorageApiTable(self::$storageApi, $this->bucketId . '.c-' . $this->dataBucketName . '_' . $filteredTableName, null, 'name');
		$table->setAttribute('tableId', $this->dataBucketId . '.' . $filteredTableName);
		$table->setAttribute('gdName', $filteredTableName);
		$table->setAttribute('export', '1');
		$table->setHeader(array('name', 'gdName', 'type', 'dataType', 'dataTypeSize', 'schemaReference', 'reference',
			'format', 'dateDimension', 'sortLabel', 'sortOrder'));
		$table->setFromArray(array(
			array('id', 'Id', 'CONNECTION_POINT', '', '', '', '', '', '', '', ''),
			array('name', 'Name', 'ATTRIBUTE', '', '', '', '', '', '', '', ''),
			array('pid', '', 'IGNORE', '', '', '', '', '', '', '', '')
		));
		$table->save();

		$table = new StorageApiTable(self::$storageApi, $this->bucketId . '.c-' . $this->dataBucketName . '_' . $notFilteredTableName, null, 'name');
		$table->setAttribute('tableId', $this->dataBucketId . '.' . $notFilteredTableName);
		$table->setAttribute('gdName', $notFilteredTableName);
		$table->setAttribute('export', '1');
		$table->setHeader(array('name', 'gdName', 'type', 'dataType', 'dataTypeSize', 'schemaReference', 'reference',
			'format', 'dateDimension', 'sortLabel', 'sortOrder'));
		$table->setFromArray(array(
			array('id', 'Id', 'CONNECTION_POINT', '', '', '', '', '', '', '', ''),
			array('name', 'Name', 'ATTRIBUTE', '', '', '', '', '', '', '', '')
		));
		$table->save();


		// Run upload
		$jobId = $this->_processJob('/gooddata-writer/upload-table', array('tableId' => $this->dataBucketId . '.' . $notFilteredTableName));
		$response = $this->_getWriterApi('/gooddata-writer/jobs?writerId=' . $this->writerId . '&jobId=' . $jobId);
		$this->assertArrayHasKey('job', $response, "Response for writer call '/jobs?jobId=' should contain key 'job'.");
		$this->assertArrayHasKey('result', $response['job'], "Response for writer call '/jobs?jobId=' should contain key 'job.result'.");
		$this->assertArrayHasKey('status', $response['job']['result'], "Response for writer call '/jobs?jobId=' should contain key 'job.result.status'.");
		$this->assertEquals('error', $response['job']['result']['status'], "Response for writer call '/jobs?jobId=' should contain key 'job.result.status' with value 'error'.");

		$jobId = $this->_processJob('/gooddata-writer/upload-table', array('tableId' => $this->dataBucketId . '.' . $filteredTableName));
		$response = $this->_getWriterApi('/gooddata-writer/jobs?writerId=' . $this->writerId . '&jobId=' . $jobId);
		print_r($response);


		//check dataset in main project if contains all rows
		//check dataset in clone if contains only filtered rows
		//try to upload with table without filteredColumn




		// Check existence of datasets in the project
		/*self::$restApi->setCredentials(self::$configuration->bucketInfo['gd']['username'], self::$configuration->bucketInfo['gd']['password']);
		$data = self::$restApi->get('/gdc/md/' . self::$configuration->bucketInfo['gd']['pid'] . '/data/sets');
		$this->assertArrayHasKey('dataSetsInfo', $data, "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo' key.");
		$this->assertArrayHasKey('sets', $data['dataSetsInfo'], "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo.sets' key.");
		$this->assertCount(1, $data['dataSetsInfo']['sets'], "Response for GoodData API call '/data/sets' should contain key 'dataSetsInfo.sets' with one value.");
		$this->assertEquals('dataset.categories', $data['dataSetsInfo']['sets'][0]['meta']['identifier'], "GoodData project should contain dataset 'Categories'.");
		*/
	}
}
