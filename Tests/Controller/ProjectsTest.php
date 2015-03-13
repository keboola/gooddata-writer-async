<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */

namespace Keboola\GoodDataWriter\Tests\Controller;

use Keboola\GoodDataWriter\Job\Metadata\Job;
use Keboola\StorageApi\Table as StorageApiTable;
use Keboola\GoodDataWriter\GoodData\WebDav;

class ProjectsTest extends AbstractControllerTest
{


    public function testProjects()
    {
        /**
         * Create project
         */
        $this->processJob('/projects', []);

        // Check of configuration
        $clonedPid = null;
        $mainPid = null;
        foreach ($this->configuration->getProjects() as $p) {
            if (empty($p['main'])) {
                $clonedPid = $p['pid'];
            } else {
                $mainPid = $p['pid'];
            }
        }
        $this->assertNotEmpty($clonedPid, "Configuration should contain a cloned project.");


        // Check of GoodData
        $bucketAttributes = $this->configuration->bucketAttributes();
        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
        $projectInfo = $this->restApi->getProject($clonedPid);
        $this->assertArrayHasKey('project', $projectInfo, "Response for GoodData API project call should contain 'project' key.");
        $this->assertArrayHasKey('content', $projectInfo['project'], "Response for GoodData API project call should contain 'project.content' key.");
        $this->assertArrayHasKey('state', $projectInfo['project']['content'], "Response for GoodData API project call should contain 'project.content.state' key.");
        $this->assertEquals('ENABLED', $projectInfo['project']['content']['state'], "Response for GoodData API project call should contain 'project.content.state' key with value 'ENABLED'.");


        // Check of Writer API
        $responseJson = $this->getWriterApi('/projects?writerId=' . $this->writerId);
        $this->assertArrayHasKey('projects', $responseJson, "Response for writer call '/projects' should contain 'projects' key.");
        $this->assertCount(2, $responseJson['projects'], "Response for writer call '/projects' should return two projects.");
        $projectFound = false;
        foreach ($responseJson['projects'] as $p) {
            if ($p['pid'] == $clonedPid) {
                $projectFound = true;
            }
        }
        $this->assertTrue($projectFound, "Response for writer call '/projects' should return tested project.");


        /**
         * Prepare configuration for filtered tables
         */
        $this->storageApi->createBucket($this->dataBucketName, 'out', 'Writer Test');
        $filteredTableName = 'filteredTable';
        $notFilteredTableName = 'notFilteredTable';

        // Prepare data
        $table = new StorageApiTable($this->storageApi, $this->dataBucketId . '.' . $filteredTableName, null, 'id');
        $table->setHeader(['id', 'name', 'pid']);
        $table->addIndex('pid');
        $table->setFromArray([
            ['u1', 'User 1', 'x'],
            ['u2', 'User 2', $clonedPid]
        ]);
        $table->save();

        $table = new StorageApiTable($this->storageApi, $this->dataBucketId . '.' . $notFilteredTableName, null, 'id');
        $table->setHeader(['id', 'name']);
        $table->setFromArray([
            ['x1', 'X 1'],
            ['x2', 'X 2']
        ]);
        $table->save();


        // Prepare configuration
        $this->configuration->updateBucketAttribute('filterColumn', 'pid');
        $this->configuration->updateDataSetsFromSapi();

        $this->configuration->updateColumnsDefinition($this->dataBucketId . '.' . $filteredTableName, [
            [
                'name' => 'id',
                'gdName' => 'Id',
                'type' => 'CONNECTION_POINT'
            ],
            [
                'name' => 'name',
                'gdName' => 'Name',
                'type' => 'ATTRIBUTE'
            ],
            [
                'name' => 'pid',
                'gdName' => '',
                'type' => 'IGNORE'
            ]
        ]);
        $this->configuration->updateColumnsDefinition($this->dataBucketId . '.' . $notFilteredTableName, [
            [
                'name' => 'id',
                'gdName' => 'Id',
                'type' => 'CONNECTION_POINT'
            ],
            [
                'name' => 'name',
                'gdName' => 'Name',
                'type' => 'ATTRIBUTE'
            ]
        ]);



        /**
         * Upload single project
         */
        // Test if upload went only to clone
        $jobId = $this->processJob('/upload-table', ['tableId' => $this->dataBucketId . '.' . $filteredTableName, 'pid' => $clonedPid]);
        $job = $this->getJobFromElasticsearch($jobId);
        $this->assertEquals(Job::STATUS_SUCCESS, $job->getStatus());

        $bucketAttributes = $this->configuration->bucketAttributes();
        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

        $data = $this->restApi->get('/gdc/md/' . $mainPid . '/data/sets');
        $this->assertArrayHasKey('dataSetsInfo', $data, "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo' key.");
        $this->assertArrayHasKey('sets', $data['dataSetsInfo'], "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo.sets' key.");
        $this->assertCount(0, $data['dataSetsInfo']['sets'], "Response for GoodData API call '/data/sets' should contain key 'dataSetsInfo.sets' with no values.");

        $data = $this->restApi->get('/gdc/md/' . $clonedPid . '/data/sets');
        $this->assertArrayHasKey('dataSetsInfo', $data, "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo' key.");
        $this->assertArrayHasKey('sets', $data['dataSetsInfo'], "Response for GoodData API call '/data/sets' should contain 'dataSetsInfo.sets' key.");
        $this->assertCount(1, $data['dataSetsInfo']['sets'], "Response for GoodData API call '/data/sets' should contain key 'dataSetsInfo.sets' with one value.");
        $this->assertArrayHasKey('lastUpload', $data['dataSetsInfo']['sets'][0], "Response for GoodData API call '/data/sets' for main project should contain key 'dataSetsInfo.sets..lastUpload'.");
        $this->assertNotEmpty($data['dataSetsInfo']['sets'][0]['lastUpload'], "Response for GoodData API call '/data/sets' for main project should contain non-empty key 'dataSetsInfo.sets..lastUpload'.");
        $this->assertArrayHasKey('dataUploadShort', $data['dataSetsInfo']['sets'][0]['lastUpload'], "Response for GoodData API call '/data/sets' for main project should contain key 'dataSetsInfo.sets..lastUpload.dataUploadShort'.");
        $this->assertArrayHasKey('status', $data['dataSetsInfo']['sets'][0]['lastUpload']['dataUploadShort'], "Response for GoodData API call '/data/sets' for clone project should contain key 'dataSetsInfo.sets..lastUpload.dataUploadShort.status'.");
        $this->assertEquals('OK', $data['dataSetsInfo']['sets'][0]['lastUpload']['dataUploadShort']['status'], "Response for GoodData API call '/data/sets' for clone project should contain key 'dataSetsInfo.sets..lastUpload.status' with value 'OK'.");



        /**
         * Filtered tables
         */
        // Test if upload of not-filtered table without 'ignoreFilter' attribute fails
        $jobId = $this->processJob('/upload-table', ['tableId' => $this->dataBucketId . '.' . $notFilteredTableName]);
        $job = $this->getJobFromElasticsearch($jobId);
        $this->assertEquals(Job::STATUS_ERROR, $job->getStatus());

        // Now add the attribute and try if it succeeds
        $this->configuration->updateDataSetDefinition($this->dataBucketId . '.' . $notFilteredTableName, 'ignoreFilter', 1);

        $jobId = $this->processJob('/upload-table', ['tableId' => $this->dataBucketId . '.' . $notFilteredTableName]);
        $job = $this->getJobFromElasticsearch($jobId);
        $this->assertEquals(Job::STATUS_SUCCESS, $job->getStatus());


        // Upload and test filtered tables
        $jobId = $this->processJob('/upload-table', ['tableId' => $this->dataBucketId . '.' . $filteredTableName]);
        $job = $this->getJobFromElasticsearch($jobId);
        $this->assertEquals(Job::STATUS_SUCCESS, $job->getStatus());

        $bucketAttributes = $this->configuration->bucketAttributes();
        $webDav = new WebDav($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);


        $checkMainProjectLoad = false;
        $checkFilteredProjectLoad = false;
        foreach ($job->getParams()['tasks'] as $i => $task) {
            if ($task['name'] == 'loadData') {
                $fileUri = sprintf('%s/%d/%s.csv', $job->getId(), $i, $task['params']['tableId']);
                $csv = $webDav->get($fileUri);
                if (!$csv) {
                    $this->assertTrue(false, sprintf("Data csv file in WebDav '%s' should exist.", $fileUri));
                }
                $rowsNumber = 0;
                foreach (explode("\n", $csv) as $row) {
                    if ($row) {
                        $rowsNumber++;
                    }
                }

                if ($task['params']['pid'] == $bucketAttributes['gd']['pid']) {
                    $this->assertEquals(3, $rowsNumber, "Csv of main project should contain two rows with header.");
                    $checkMainProjectLoad = true;
                } else {
                    $this->assertEquals(2, $rowsNumber, "Csv of cloned project should contain only one row with header.");
                    $checkFilteredProjectLoad = true;
                }
            }
        }
        $this->assertTrue($checkMainProjectLoad, 'Data load task of main project was not found in the job');
        $this->assertTrue($checkFilteredProjectLoad, 'Data load task of filtered project was not found in the job');



        /**
         * reset project
         */
        $bucketAttributes = $this->configuration->bucketAttributes();
        $oldPid = (string)$bucketAttributes['gd']['pid'];

        $this->processJob('/reset-project');
        $bucketAttributes = $this->configuration->bucketAttributes();
        $newPid = (string)$bucketAttributes['gd']['pid'];
        $this->assertNotEquals($newPid, $oldPid, 'Project reset failed');

        $this->processJob('/reset-project', ['removeClones' => true]);
        $allProjects = $this->configuration->getProjects();
        $this->assertCount(1, $allProjects, 'Reset of project clones failed');
    }
}
