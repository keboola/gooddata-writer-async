<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
namespace Keboola\GoodDataWriter\Tests\Controller;

use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\GoodData\WebDav;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Keboola\StorageApi\Table as StorageApiTable;

class DatasetsTest extends AbstractControllerTest
{

    public function testDatasets()
    {
        $this->prepareData();
        $bucketAttributes = $this->configuration->bucketAttributes();
        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
        $webDav = new WebDav($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);


        /**
         * Upload single table
         */
        $batchId = $this->processJob('/upload-table', array('tableId' => $this->dataBucketId . '.categories'));
        $response = $this->getWriterApi('/batch?writerId=' . $this->writerId . '&batchId=' . $batchId);
        $lastJob = end($response['jobs']);
        $jobId = $lastJob['id'];

        // Check existence of datasets in the project
        $data = $this->restApi->get('/gdc/md/' . $bucketAttributes['gd']['pid'] . '/data/sets');
        $this->assertArrayHasKey('dataSetsInfo', $data, "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo' key.");
        $this->assertArrayHasKey('sets', $data['dataSetsInfo'], "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo.sets' key.");
        $this->assertCount(1, $data['dataSetsInfo']['sets'], "Response for GoodData API call '/data/sets' should contain key 'dataSetsInfo.sets' with one value.");
        $this->assertEquals('dataset.categories', $data['dataSetsInfo']['sets'][0]['meta']['identifier'], "GoodData project should contain dataSet 'Categories'.");

        $csv = $webDav->get(sprintf('%s/categories.csv', $jobId));

        if (!$csv) {
            $this->assertTrue(false, sprintf("Data csv file in WebDav '/uploads/%s/categories.csv' should exist.", $jobId));
        }
        $rows = StorageApiClient::parseCsv($csv);
        $this->assertEquals(2, count($rows), "Csv of main project should contain two rows.");

        $categoriesFound = false;
        $categoriesDataLoad = false;
        $data = $this->restApi->get('/gdc/md/' . $bucketAttributes['gd']['pid'] . '/data/sets');
        foreach ($data['dataSetsInfo']['sets'] as $d) {
            if ($d['meta']['identifier'] == 'dataset.categories') {
                $categoriesFound = true;
                if ($d['lastUpload']['dataUploadShort']['status'] == 'OK') {
                    $categoriesDataLoad = true;
                }
            }
        }
        $this->assertTrue($categoriesFound, "Dataset 'Categories' has not been found in GoodData");
        $this->assertTrue($categoriesDataLoad, "Data to dataset 'Categories' has not been loaded to GoodData");



        /**
         * Upload whole project
         */
        $this->processJob('/upload-project');

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


        // Check validity of foreign keys (including time dimension during daylight saving switch values)
        $result = $this->restApi->validateProject($bucketAttributes['gd']['pid']);
        $this->assertEquals(0, $result['error_found'], 'Project validation should not contain errors but result is: ' . print_r($result, true));
        $this->assertEquals(0, $result['fatal_error_found'], 'Project validation should not contain errors but result is: ' . print_r($result, true));


        /**
         * Multi load
         */
        $batchId = $this->processJob('/load-data-multi', array('tables' => array($this->dataBucketId . '.products', $this->dataBucketId . '.categories')));
        $responseJson = $this->getWriterApi('/batch?writerId=' . $this->writerId . '&batchId=' . $batchId);
        $this->assertArrayHasKey('status', $responseJson, "Response for GoodData API call '/batch' should contain 'status' key.");
        $this->assertEquals('success', $responseJson['status'], "Batch '$batchId' should have status 'success'.");

        /**
         * Check if upload table contains updateModel when needed
         */
        $tableId = $this->dataBucketId . '.products';
        $response = $this->postWriterApi('/upload-table', array(
            'writerId' => $this->writerId,
            'tableId' => $tableId
        ));

        $batch = $this->getWriterApi('/batch?writerId=' . $this->writerId . '&batchId=' . $response['batch']);
        $this->assertCount(1, $batch['jobs'], 'Upload table should contain only loadData job');

        $this->configuration->updateColumnsDefinition($tableId, 'id', array('gdName' => 'ID'));

        $response = $this->postWriterApi('/upload-table', array(
            'writerId' => $this->writerId,
            'tableId' => $tableId
        ));
        $batch = $this->getWriterApi('/batch?writerId=' . $this->writerId . '&batchId=' . $response['batch']);
        $this->assertCount(2, $batch['jobs'], 'Upload table should contain updateModel and loadData job');

        $table = new StorageApiTable($this->storageApi, 'sys.c-wr-gooddata-' . $this->writerId . '.data_sets', null, 'id');
        $table->setHeader(array('id', 'lastChangeDate'));
        $table->setFromArray(array(
            array($tableId, date('c', time()-86400))
        ));
        $table->setIncremental(true);
        $table->setPartial(true);
        $table->save();

        $response = $this->postWriterApi('/upload-table', array(
            'writerId' => $this->writerId,
            'tableId' => $tableId
        ));
        $batch = $this->getWriterApi('/batch?writerId=' . $this->writerId . '&batchId=' . $response['batch']);
        $this->assertCount(1, $batch['jobs'], 'Upload table should contain only loadData job');


        /**
         * Check if upload project contains updateModel when needed
         */
        $response = $this->postWriterApi('/upload-project', array(
            'writerId' => $this->writerId
        ));

        $batch = $this->getWriterApi('/batch?writerId=' . $this->writerId . '&batchId=' . $response['batch']);
        $this->assertCount(3, $batch['jobs'], 'Upload project should contain two loadData jobs');

        $this->configuration->updateColumnsDefinition($tableId, 'id', array('gdName' => 'ID'));

        $response = $this->postWriterApi('/upload-project', array(
            'writerId' => $this->writerId
        ));
        $batch = $this->getWriterApi('/batch?writerId=' . $this->writerId . '&batchId=' . $response['batch']);
        $this->assertCount(4, $batch['jobs'], 'Upload project should contain one updateModel and two loadData jobs');

        $table = new StorageApiTable($this->storageApi, 'sys.c-wr-gooddata-' . $this->writerId . '.data_sets', null, 'id');
        $table->setHeader(array('id', 'lastChangeDate'));
        $table->setFromArray(array(
            array($tableId, date('c', time()-86400))
        ));
        $table->setIncremental(true);
        $table->setPartial(true);
        $table->save();

        $response = $this->postWriterApi('/upload-project', array(
            'writerId' => $this->writerId
        ));
        $batch = $this->getWriterApi('/batch?writerId=' . $this->writerId . '&batchId=' . $response['batch']);
        $this->assertCount(3, $batch['jobs'], 'Upload project should contain two loadData jobs');



        // Check validity of foreign keys (including time dimension during daylight saving switch values)
        $result = $this->restApi->validateProject($bucketAttributes['gd']['pid']);
        $this->assertEquals(0, $result['error_found'], 'Project validation should not contain errors but result is: ' . print_r($result, true));
        $this->assertEquals(0, $result['fatal_error_found'], 'Project validation should not contain errors but result is: ' . print_r($result, true));


        /**
         * Get all tables
         */
        $responseJson = $this->getWriterApi('/tables?writerId=' . $this->writerId);

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
            $this->assertArrayHasKey('name', $table, sprintf("Table '%s' should have 'type' attribute.", $table['id']));
            $this->assertTrue(in_array($table['name'], array('Products', 'Categories')), sprintf("Table '%s' does not belong to configured tables.", $table['id']));
            $this->assertArrayHasKey('export', $table, sprintf("Table '%s' should have 'export' attribute.", $table['id']));
            $this->assertArrayHasKey('isExported', $table, sprintf("Table '%s' should have 'isExported' attribute.", $table['id']));
        }


        /**
         * Get specific table
         */
        $responseJson = $this->getWriterApi('/tables?writerId=' . $this->writerId . '&tableId=' . $this->dataBucketId . '.products');

        $this->assertArrayHasKey('table', $responseJson, "Response for writer call '/tables?tableId=' should contain 'table' key.");
        $this->assertArrayHasKey('id', $responseJson['table'], "Response for writer call '/tables?tableId=' should contain 'table.id' key.");
        $this->assertArrayHasKey('name', $responseJson['table'], "Response for writer call '/tables?tableId=' should contain 'table.name' key.");
        $this->assertArrayHasKey('columns', $responseJson['table'], "Response for writer call '/tables?tableId=' should contain 'table.columns' key.");
        $this->assertEquals($this->dataBucketId . '.products', $responseJson['table']['id'], "Response for writer call '/tables?tableId=' should contain 'table.id' key with value of data bucket Products.");
        $this->assertCount(5, $responseJson['table']['columns'], "Response for writer call '/tables?tableId=' should contain 'table.columns' key with five columns.");


        /**
         * Get tables with connection points
         */
        $responseJson = $this->getWriterApi('/tables?writerId=' . $this->writerId . '&connection');

        $this->assertArrayHasKey('tables', $responseJson, "Response for writer call '/tables?connection' should contain 'tables' key.");
        $this->assertCount(2, $responseJson['tables'], "Response for writer call '/tables?connection' should contain two configured tables.");



        /**
         * Change table definition
         */
        $tableId = $this->dataBucketId . '.categories';
        $testName = uniqid('test-name');

        // Change gdName of table
        $this->postWriterApi('/tables', array(
            'writerId' => $this->writerId,
            'tableId' => $tableId,
            'name' => $testName
        ));

        // Check if GD name was changed
        $responseJson = $this->getWriterApi('/tables?writerId=' . $this->writerId);
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
        $this->postWriterApi('/tables', array(
            'writerId' => $this->writerId,
            'tableId' => $tableId,
            'name' => $testName . '2'
        ));

        $responseJson = $this->getWriterApi('/tables?writerId=' . $this->writerId);
        $lastChangeDateAfterUpdate = null;
        foreach ($responseJson['tables'] as $t) {
            if ($t['id'] != $tableId) {
                continue;
            }
            $lastChangeDateAfterUpdate = $t['lastChangeDate'];
        }

        $this->assertNotEquals($lastChangeDate, $lastChangeDateAfterUpdate, 'Last change date should be changed after update');


        // Change gdName back
        $this->postWriterApi('/tables', array(
            'writerId' => $this->writerId,
            'tableId' => $tableId,
            'name' => 'categories'
        ));


        /**
         * Change column definition
         */
        $tableId = $this->dataBucketId . '.products';
        $columnName = 'id';
        $newGdName = 'test' . uniqid();
        $this->postWriterApi('/tables', array(
            'writerId' => $this->writerId,
            'tableId' => $tableId,
            'column' => $columnName,
            'gdName' => $newGdName
        ));
        $responseJson = $this->getWriterApi('/tables?writerId=' . $this->writerId . '&tableId=' . $tableId);

        $this->assertArrayHasKey('table', $responseJson, "Response for writer call '/tables&tableId=' should contain 'table' key.");
        $this->assertArrayHasKey('columns', $responseJson['table'], "Response for writer call '/tables&tableId=' should contain 'table.columns' key.");
        $columnFound = false;
        foreach ($responseJson['table']['columns'] as $column) {
            if ($column['name'] == $columnName) {
                $columnFound = true;
                $this->assertEquals($newGdName, $column['gdName'], sprintf("GdName of column '%s' should be changed to '%s'", $columnName, $newGdName));
                break;
            }
        }
        $this->assertTrue($columnFound, sprintf("Response for writer call '/tables&tableId=' should contain '%s' column.", $columnName));


        /**
         * Change multiple columns definition
         */
        $columnName1 = 'id';
        $newGdName1 = 'test' . uniqid();
        $columnName2 = 'name';
        $newGdName2 = 'test' . uniqid();
        $this->postWriterApi('/tables', array(
            'writerId' => $this->writerId,
            'tableId' => $tableId,
            'columns' => array(
                array(
                    'name' => $columnName1,
                    'gdName' => $newGdName1,
                ),
                array(
                    'name' => $columnName2,
                    'gdName' => $newGdName2,
                )
            )
        ));
        $responseJson = $this->getWriterApi('/tables?writerId=' . $this->writerId . '&tableId=' . $tableId);
        $this->assertArrayHasKey('table', $responseJson, "Response for writer call '/tables&tableId=' should contain 'table' key.");
        $this->assertArrayHasKey('columns', $responseJson['table'], "Response for writer call '/tables&tableId=' should contain 'table.columns' key.");
        $column1Found = false;
        $column2Found = false;
        foreach ($responseJson['table']['columns'] as $column) {
            if ($column['name'] == $columnName1) {
                $column1Found = true;
                $this->assertEquals($newGdName1, $column['gdName'], sprintf("GdName of column '%s' should be changed to '%s'", $columnName1, $newGdName1));
            }
            if ($column['name'] == $columnName2) {
                $column2Found = true;
                $this->assertEquals($newGdName2, $column['gdName'], sprintf("GdName of column '%s' should be changed to '%s'", $columnName2, $newGdName2));
            }
        }
        $this->assertTrue($column1Found, sprintf("Response for writer call '/tables&tableId=' should contain '%s' column.", $columnName1));
        $this->assertTrue($column2Found, sprintf("Response for writer call '/tables&tableId=' should contain '%s' column.", $columnName2));


        /**
         * Set column to ignore and check outgoing csv
         */
        $tableId = $this->dataBucketId . '.products';
        $this->configuration->updateColumnsDefinition($tableId, 'price', array('type' => 'IGNORE'));
        $batchId = $this->processJob('/upload-table', array('tableId' => $tableId));

        $response = $this->getWriterApi('/batch?writerId=' . $this->writerId . '&batchId=' . $batchId);
        $lastJob = end($response['jobs']);
        $jobId = $lastJob['id'];

        $csv = $webDav->get(sprintf('%s/products.csv', $jobId));
        if (!$csv) {
            $this->assertTrue(false, sprintf("Data csv file in WebDav '/uploads/%s/products.csv' should exist.", $jobId));
        }
        $rows = StorageApiClient::parseCsv($csv);
        foreach ($rows as $row) {
            // csv will contain also date facts, therefore number 6 (3 columns are just for date)
            $this->assertCount(6, $row, 'Table should contain columns without products');
        }


        /**
         * Remove column from out table
         */
        $tableId = $this->dataBucketId . '.products';
        $nowTime = date('c');

        // Remove column and test if lastChangeDate changed
        $this->storageApi->deleteTableColumn($tableId, 'price');

        $responseJson = $this->getWriterApi('/tables?writerId=' . $this->writerId . '&tableId=' . $tableId);
        $this->assertArrayHasKey('table', $responseJson, "Response for writer call '/tables&tableId=' should contain 'table' key.");

        $this->assertArrayHasKey('lastChangeDate', $responseJson['table'], "Response for writer call '/tables&tableId=' should contain 'table.lastChangeDate' key.");
        $this->assertGreaterThan($nowTime, $responseJson['table']['lastChangeDate'], "Response for writer call '/tables&tableId=' should have 'table.lastChangeDate' updated.");



        /**
         * Upload table with model changes
         */
        $batchId = $this->processJob('/upload-table', array('tableId' => $tableId));
        $response = $this->getWriterApi('/batch?writerId=' . $this->writerId . '&batchId=' . $batchId);
        $this->assertArrayHasKey('status', $response, "Response for writer call '/batch' should contain key 'status'.");
        $this->assertEquals(SharedStorage::JOB_STATUS_SUCCESS, $response['status'], "Response for writer call '/batch' should contain key 'status' with value 'success'.");


        /**
         * Reset table and remove dataset from GoodData
         */
        $tableId = $this->dataBucketId . '.products';
        $this->processJob('/reset-table', array('tableId' => $tableId));
        $data = $this->restApi->get('/gdc/md/' . $bucketAttributes['gd']['pid'] . '/data/sets');
        $this->assertArrayHasKey('dataSetsInfo', $data, "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo' key.");
        $this->assertArrayHasKey('sets', $data['dataSetsInfo'], "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo.sets' key.");

        $productsFound = false;
        foreach ($data['dataSetsInfo']['sets'] as $d) {
            if ($d['meta']['identifier'] == 'dataset.products') {
                $productsFound = true;
            }
        }
        $this->assertFalse($productsFound, "Dataset products was not removed from GoodData");
        $responseJson = $this->getWriterApi('/tables?writerId=' . $this->writerId . '&tableId=' . $tableId);

        $this->assertArrayHasKey('table', $responseJson, "Response for writer call '/tables&tableId=' should contain 'table' key.");
        $this->assertArrayHasKey('isExported', $responseJson['table'], "Response for writer call '/tables&tableId=' should contain 'table.isExported' key.");
        $this->assertFalse((bool)$responseJson['table']['isExported'], "IsExported flag of reset table should be false.");



        /**
         * Date Dimensions
         */

        // Create dimension
        $dimensionName = 'TestDate';
        $templateName = 'keboola';
        $this->postWriterApi('/date-dimensions', array(
            'writerId' => $this->writerId,
            'name' => $dimensionName,
            'includeTime' => true,
            'template' => $templateName
        ));

        // Get dimensions
        $responseJson = $this->getWriterApi('/date-dimensions?writerId=' . $this->writerId . '&usage');
        $this->assertArrayHasKey('dimensions', $responseJson, "Response for writer call '/date-dimensions' should contain 'dimensions' key.");
        $this->assertCount(2, $responseJson['dimensions'], "Response for writer call '/date-dimensions' should contain two dimensions.");
        $this->assertArrayHasKey($dimensionName, $responseJson['dimensions'], "Response for writer call '/date-dimensions' should contain dimension 'TestDate'.");
        $this->assertArrayHasKey('ProductDate', $responseJson['dimensions'], "Response for writer call '/date-dimensions' should contain dimension 'ProductDate'.");
        $this->assertArrayHasKey('usedIn', $responseJson['dimensions']['ProductDate'], "Response for writer call '/date-dimensions' should contain key 'usedIn' for dimension 'ProductDate'.");
        $this->assertCount(1, $responseJson['dimensions']['ProductDate']['usedIn'], "Response for writer call '/date-dimensions' should contain usage of dimension 'ProductDate'.");
        $this->assertEquals($this->dataBucketId . '.products', $responseJson['dimensions']['ProductDate']['usedIn'][0], "Response for writer call '/date-dimensions' should contain usage of dimension 'ProductDate' in dataset 'Products'.");


        // Upload created date do GoodData
        $jobId = $this->processJob('/upload-date-dimension', array('tableId' => $tableId, 'name' => $dimensionName));
        $response = $this->getWriterApi('/batch?writerId=' . $this->writerId . '&batchId=' . $jobId);
        $this->assertArrayHasKey('status', $response, "Response for writer call '/batch?batchId=' should contain key 'job.status'.");
        $this->assertEquals(SharedStorage::JOB_STATUS_SUCCESS, $response['status'], "Result of request /upload-date-dimension should be 'success'.");

        $data = $this->restApi->get('/gdc/md/' . $bucketAttributes['gd']['pid'] . '/data/sets');
        $this->assertArrayHasKey('dataSetsInfo', $data, "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo' key.");
        $this->assertArrayHasKey('sets', $data['dataSetsInfo'], "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo.sets' key.");

        $dateFound = false;
        $dateTimeFound = false;
        $dateTimeDataLoad = false;
        foreach ($data['dataSetsInfo']['sets'] as $d) {
            if ($d['meta']['identifier'] == Model::getTimeDimensionId($dimensionName)) {
                $dateTimeFound = true;
                if ($d['lastUpload']['dataUploadShort']['status'] == 'OK') {
                    $dateTimeDataLoad = true;
                }
            }
            if ($d['meta']['identifier'] == Model::getDateDimensionId($dimensionName, $templateName)) {
                $dateFound = true;
            }
        }
        $this->assertTrue($dateFound, sprintf("Date dimension '%s' has not been found in GoodData", $dimensionName));
        $this->assertTrue($dateTimeFound, sprintf("Time dimension '%s' has not been found in GoodData", $dimensionName));
        $this->assertTrue($dateTimeDataLoad, sprintf("Time dimension '%s' has not been successfully loaded to GoodData", $dimensionName));


        // Drop dimension
        $dimensionName = 'TestDate_' . uniqid();
        $this->postWriterApi('/date-dimensions', array(
            'writerId' => $this->writerId,
            'name' => $dimensionName,
            'includeTime' => true
        ));
        $this->callWriterApi('/date-dimensions?writerId=' . $this->writerId . '&name=' . $dimensionName, 'DELETE');
        $responseJson = $this->getWriterApi('/date-dimensions?writerId=' . $this->writerId);
        $this->assertCount(2, $responseJson['dimensions'], "Response for writer call '/date-dimensions' should contain one dimension.");
        $this->assertArrayNotHasKey($dimensionName, $responseJson['dimensions'], "Response for writer call '/date-dimensions' should not contain dimension 'TestDate'.");
    }
}
