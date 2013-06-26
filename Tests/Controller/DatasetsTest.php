<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
namespace Keboola\GoodDataWriter\Tests\Controller;

use Keboola\GoodDataWriter\Test\WriterTest,
	Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\StorageApi\Table as StorageApiTable;

class DatasetsTest extends WriterTest
{

	public function testUploadProject()
	{
		$this->_prepareData();

		$this->_processJob('/gooddata-writer/upload-project');

		// Check existence of datasets in the project
		self::$restApi->login(self::$configuration->bucketInfo['gd']['username'], self::$configuration->bucketInfo['gd']['password']);
		$data = self::$restApi->get('/gdc/md/' . self::$configuration->bucketInfo['gd']['pid'] . '/data/sets');
		$this->assertArrayHasKey('dataSetsInfo', $data, "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo' key.");
		$this->assertArrayHasKey('sets', $data['dataSetsInfo'], "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo.sets' key.");
		$this->assertCount(3, $data['dataSetsInfo']['sets'], "Response for GoodData API call '/data/sets' should contain key 'dataSetsInfo.sets' with three values.");
	}


	/*public function testUploadTable()
	{
		//@TODO need to test that load data finished successfully
	}*/


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
}
