<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */

namespace Keboola\GoodDataWriter\Tests\Controller;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Table as StorageApiTable;

class ProjectsTest extends AbstractControllerTest
{


	public function testCreateProject()
	{
		$this->_processJob('/gooddata-writer/projects', array());

		// Check of configuration
		$projectsList = self::$configuration->getProjects();
		$this->assertCount(2, $projectsList, "Response for writer call '/projects' should return two GoodData projects.");
		$project = $projectsList[1];


		// Check of GoodData
		self::$restApi->setCredentials(self::$configuration->bucketInfo['gd']['username'], self::$configuration->bucketInfo['gd']['password']);
		$projectInfo = self::$restApi->getProject($project['pid']);
		$this->assertArrayHasKey('project', $projectInfo, "Response for GoodData API project call should contain 'project' key.");
		$this->assertArrayHasKey('content', $projectInfo['project'], "Response for GoodData API project call should contain 'project.content' key.");
		$this->assertArrayHasKey('state', $projectInfo['project']['content'], "Response for GoodData API project call should contain 'project.content.state' key.");
		$this->assertEquals('ENABLED', $projectInfo['project']['content']['state'], "Response for GoodData API project call should contain 'project.content.state' key with value 'ENABLED'.");


		// Check of Writer API
		$responseJson = $this->_getWriterApi('/gooddata-writer/projects?writerId=' . $this->writerId);
		$this->assertArrayHasKey('projects', $responseJson, "Response for writer call '/projects' should contain 'projects' key.");
		$this->assertCount(2, $responseJson['projects'], "Response for writer call '/projects' should return two projects.");
		$projectFound = false;
		foreach ($responseJson['projects'] as $p) {
			if ($p['pid'] == $project['pid']) {
				$projectFound = true;
			}
		}
		$this->assertTrue($projectFound, "Response for writer call '/projects' should return tested project.");
	}


	public function testUploadFilteredTable()
	{
		self::$storageApi->createBucket($this->dataBucketName, 'out', 'Writer Test');
		$filteredTableName = 'filteredTable';
		$notFilteredTableName = 'notFilteredTable';


		// Clone project
		$this->_processJob('/gooddata-writer/projects', array());
		$clonedPid = null;
		$mainPid = null;
		foreach (self::$configuration->getProjects() as $p) if (empty($p['main'])) {
			$clonedPid = $p['pid'];
		} else {
			$mainPid = $p['pid'];
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

		self::$configuration->createTableDefinition($this->dataBucketId . '.' . $filteredTableName);
		self::$configuration->saveColumnDefinition($this->dataBucketId . '.' . $filteredTableName,
			array('name' => 'id', 'gdName' => 'Id', 'type' => 'CONNECTION_POINT'));
		self::$configuration->saveColumnDefinition($this->dataBucketId . '.' . $filteredTableName,
			array('name' => 'name', 'gdName' => 'Name', 'type' => 'ATTRIBUTE'));
		self::$configuration->saveColumnDefinition($this->dataBucketId . '.' . $filteredTableName,
			array('name' => 'pid', 'gdName' => '', 'type' => 'IGNORE'));

		self::$configuration->createTableDefinition($this->dataBucketId . '.' . $notFilteredTableName);
		self::$configuration->saveColumnDefinition($this->dataBucketId . '.' . $notFilteredTableName,
			array('name' => 'id', 'gdName' => 'Id', 'type' => 'CONNECTION_POINT'));
		self::$configuration->saveColumnDefinition($this->dataBucketId . '.' . $notFilteredTableName,
			array('name' => 'name', 'gdName' => 'Name', 'type' => 'ATTRIBUTE'));


		// Test if upload of not-filtered table without 'ignoreFilter' attribute fails
		$jobId = $this->_processJob('/gooddata-writer/upload-table', array('tableId' => $this->dataBucketId . '.' . $notFilteredTableName));
		$response = $this->_getWriterApi('/gooddata-writer/jobs?writerId=' . $this->writerId . '&jobId=' . $jobId);
		$this->assertArrayHasKey('job', $response, "Response for writer call '/jobs?jobId=' should contain key 'job'.");
		$this->assertArrayHasKey('result', $response['job'], "Response for writer call '/jobs?jobId=' should contain key 'job.result'.");
		$this->assertArrayHasKey('status', $response['job']['result'], "Response for writer call '/jobs?jobId=' should contain key 'job.result.status'.");
		$this->assertEquals('error', $response['job']['result']['status'], "Response for writer call '/jobs?jobId=' should contain key 'job.result.status' with value 'error'.");

		// Now add the attribute and try if it succeeds
		self::$configuration->setTableAttribute($this->dataBucketId . '.' . $notFilteredTableName, 'ignoreFilter', 1);

		$jobId = $this->_processJob('/gooddata-writer/upload-table', array('tableId' => $this->dataBucketId . '.' . $notFilteredTableName));
		$response = $this->_getWriterApi('/gooddata-writer/jobs?writerId=' . $this->writerId . '&jobId=' . $jobId);
		$this->assertArrayHasKey('job', $response, "Response for writer call '/jobs?jobId=' should contain key 'job'.");
		$this->assertArrayHasKey('result', $response['job'], "Response for writer call '/jobs?jobId=' should contain key 'job.result'.");
		$this->assertArrayHasKey('status', $response['job']['result'], "Response for writer call '/jobs?jobId=' should contain key 'job.result.status'.");
		$this->assertEquals('success', $response['job']['result']['status'], "Response for writer call '/jobs?jobId=' should contain key 'job.result.status' with value 'success'.");



		// Upload and test filtered tables
		$jobId = $this->_processJob('/gooddata-writer/upload-table', array('tableId' => $this->dataBucketId . '.' . $filteredTableName));
		$response = $this->_getWriterApi('/gooddata-writer/jobs?writerId=' . $this->writerId . '&jobId=' . $jobId);
		$this->assertArrayHasKey('job', $response, "Response for writer call '/jobs?jobId=' should contain key 'job'.");
		$this->assertArrayHasKey('result', $response['job'], "Response for writer call '/jobs?jobId=' should contain key 'job.result'.");
		$this->assertArrayHasKey('status', $response['job']['result'], "Response for writer call '/jobs?jobId=' should contain key 'job.result.status'.");
		$this->assertEquals('success', $response['job']['result']['status'], "Response for writer call '/jobs?jobId=' should contain key 'job.result.status' with value 'success'.");

		// Check csv of main project if contains all rows
		$csvFile = sprintf('%s/%s/%s/data.csv', self::$mainConfig['tmp_path'], $jobId, $mainPid);
		$this->assertTrue(file_exists($csvFile), sprintf("Data csv file '%s' should exist.", $csvFile));
		$csv = new CsvFile($csvFile);
		$rowsNumber = 0;
		foreach ($csv as $row) {
			$rowsNumber++;
		}
		$this->assertEquals(3, $rowsNumber, "Csv of main project should contain two rows with header.");

		// Check csv of clone if contains only filtered rows
		$csvFile = sprintf('%s/%s/%s/data.csv', self::$mainConfig['tmp_path'], $jobId, $clonedPid);
		$this->assertTrue(file_exists($csvFile), sprintf("Data csv file '%s' should exist.", $csvFile));
		$csv = new CsvFile($csvFile);
		$rowsNumber = 0;
		foreach ($csv as $row) {
			$rowsNumber++;
		}
		$this->assertEquals(2, $rowsNumber, "Csv of cloned project should contain only one row with header.");
	}

	public function testUploadSingleProject()
	{
		$tableName = 'table';
		self::$storageApi->createBucket($this->dataBucketName, 'out', 'Writer Test');

		// Clone project
		$this->_processJob('/gooddata-writer/projects', array());
		$clonedPid = null;
		$mainPid = null;
		foreach (self::$configuration->getProjects() as $p) if (empty($p['main'])) {
			$clonedPid = $p['pid'];
		} else {
			$mainPid = $p['pid'];
		}
		$this->assertNotEmpty($clonedPid, "Configuration should contain a cloned project.");


		// Prepare data
		$table = new StorageApiTable(self::$storageApi, $this->dataBucketId . '.' . $tableName, null, 'id');
		$table->setHeader(array('id', 'name', 'pid'));
		$table->addIndex('pid');
		$table->setFromArray(array(
			array('u1', 'User 1', 'x'),
			array('u2', 'User 2', $clonedPid)
		));
		$table->save();


		// Prepare configuration
		self::$configuration->setBucketAttribute('filterColumn', 'pid');

		self::$configuration->createTableDefinition($this->dataBucketId . '.' . $tableName);
		self::$configuration->saveColumnDefinition($this->dataBucketId . '.' . $tableName,
			array('name' => 'id', 'gdName' => 'Id', 'type' => 'CONNECTION_POINT'));
		self::$configuration->saveColumnDefinition($this->dataBucketId . '.' . $tableName,
			array('name' => 'name', 'gdName' => 'Name', 'type' => 'ATTRIBUTE'));
		self::$configuration->saveColumnDefinition($this->dataBucketId . '.' . $tableName,
			array('name' => 'pid', 'gdName' => '', 'type' => 'IGNORE'));


		// Test if upload went only to clone
		$jobId = $this->_processJob('/gooddata-writer/upload-table', array('tableId' => $this->dataBucketId . '.' . $tableName, 'pid' => $clonedPid));
		$response = $this->_getWriterApi('/gooddata-writer/jobs?writerId=' . $this->writerId . '&jobId=' . $jobId);
		$this->assertArrayHasKey('job', $response, "Response for writer call '/jobs?jobId=' should contain key 'job'.");
		$this->assertArrayHasKey('result', $response['job'], "Response for writer call '/jobs?jobId=' should contain key 'job.result'.");
		$this->assertArrayHasKey('status', $response['job']['result'], "Response for writer call '/jobs?jobId=' should contain key 'job.result.status'.");
		$this->assertEquals('success', $response['job']['result']['status'], "Response for writer call '/jobs?jobId=' should contain key 'job.result.status' with value 'success'.");

		self::$restApi->setCredentials(self::$configuration->bucketInfo['gd']['username'], self::$configuration->bucketInfo['gd']['password']);
		$data = self::$restApi->get('/gdc/md/' . $mainPid . '/data/sets');print_r($data);
		$this->assertArrayHasKey('dataSetsInfo', $data, "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo' key.");
		$this->assertArrayHasKey('sets', $data['dataSetsInfo'], "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo.sets' key.");
		$this->assertCount(1, $data['dataSetsInfo']['sets'], "Response for GoodData API call '/data/sets' should contain key 'dataSetsInfo.sets' with one value.");
		$this->assertArrayHasKey('lastUpload', $data['dataSetsInfo']['sets'][0], "Response for GoodData API call '/data/sets' for main project should contain key 'dataSetsInfo.sets..lastUpload'.");
		$this->assertEmpty($data['dataSetsInfo']['sets'][0]['lastUpload'], "Response for GoodData API call '/data/sets' for main project should contain empty key 'dataSetsInfo.sets..lastUpload'.");

		self::$restApi->setCredentials(self::$configuration->bucketInfo['gd']['username'], self::$configuration->bucketInfo['gd']['password']);
		$data = self::$restApi->get('/gdc/md/' . $clonedPid . '/data/sets');print_r($data);
		$this->assertArrayHasKey('dataSetsInfo', $data, "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo' key.");
		$this->assertArrayHasKey('sets', $data['dataSetsInfo'], "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo.sets' key.");
		$this->assertCount(1, $data['dataSetsInfo']['sets'], "Response for GoodData API call '/data/sets' should contain key 'dataSetsInfo.sets' with one value.");
		$this->assertArrayHasKey('lastUpload', $data['dataSetsInfo']['sets'][0], "Response for GoodData API call '/data/sets' for main project should contain key 'dataSetsInfo.sets..lastUpload'.");
		$this->assertNotEmpty($data['dataSetsInfo']['sets'][0]['lastUpload'], "Response for GoodData API call '/data/sets' for main project should contain non-empty key 'dataSetsInfo.sets..lastUpload'.");
		$this->assertArrayHasKey('dataUploadShort', $data['dataSetsInfo']['sets'][0]['lastUpload'], "Response for GoodData API call '/data/sets' for main project should contain key 'dataSetsInfo.sets..lastUpload.dataUploadShort'.");
		$this->assertArrayHasKey('status', $data['dataSetsInfo']['sets'][0]['lastUpload']['dataUploadShort'], "Response for GoodData API call '/data/sets' for main project should contain key 'dataSetsInfo.sets..lastUpload.dataUploadShort.status'.");
		$this->assertEquals('OK', $data['dataSetsInfo']['sets'][0]['lastUpload']['dataUploadShort']['status'], "Response for GoodData API call '/data/sets' for main project should contain key 'dataSetsInfo.sets..lastUpload.status' with value 'OK'.");
	}

}
