<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */

namespace Keboola\GoodDataWriter\Tests\Controller;

use Keboola\GoodDataWriter\Elasticsearch\Search;
use Keboola\GoodDataWriter\Job\Metadata\Job;
use Keboola\GoodDataWriter\Writer\Configuration;
use Keboola\Syrup\Exception\UserException;

class WritersTest extends AbstractControllerTest
{

    public function testWriters()
    {
        /**
         * Create writer
         */
        // Check writer configuration
        $bucketAttributes = false;
        try {
            $bucketAttributes = $this->configuration->getBucketAttributes();
        } catch (UserException $e) {
            $this->fail("Writer configuration is not valid.");
        }

        // Check project existence in GD
        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
        $projectInfo = $this->restApi->getProject($bucketAttributes['gd']['pid']);
        $this->assertArrayHasKey('project', $projectInfo, "Response for GoodData API login call should contain 'project' key.");
        $this->assertArrayHasKey('content', $projectInfo['project'], "Response for GoodData API login call should contain 'project.content' key.");
        $this->assertArrayHasKey('state', $projectInfo['project']['content'], "Response for GoodData API login call should contain 'project.content.state' key.");
        $this->assertEquals('ENABLED', $projectInfo['project']['content']['state'], "Response for GoodData API login call should have key 'project.content.state' with value 'ENABLED'.");

        // Check user existence in GD
        $userInfo = $this->restApi->getUser($bucketAttributes['gd']['uid']);
        $this->assertArrayHasKey('accountSetting', $userInfo, "Response for GoodData API user info call should contain 'accountSetting' key.");

        // Check user's access to the project in GD
        $userProjectsInfo = $this->restApi->get(sprintf('/gdc/account/profile/%s/projects', $bucketAttributes['gd']['uid']));
        $this->assertArrayHasKey('projects', $userProjectsInfo, "Response for GoodData API user projects call should contain 'projects' key.");
        $this->assertCount(1, $userProjectsInfo['projects'], "Writer's primary user should have exactly one project assigned.");
        $projectFound = false;
        foreach ($userProjectsInfo['projects'] as $p) {
            if (isset($p['project']['links']['metadata']) && strpos($p['project']['links']['metadata'], $bucketAttributes['gd']['pid']) !== false) {
                $projectFound = true;
                break;
            }
        }
        $this->assertTrue($projectFound, "Writer's primary user should have assigned master project.");



        /**
         * Create writer with invitations
         */
        $uniqId = uniqid();
        $writerId = self::WRITER_ID_PREFIX . 'invit_' . $uniqId;
        $user1 = 'user1' . $uniqId . '@test.keboola.com';
        $user2 = 'user2' . $uniqId . '@test.keboola.com';
        $description = uniqId();

        // Create new writer and new configuration
        $this->processJob('/writers', [
            'writerId' => $writerId,
            'users' => $user1 . ',' . $user2,
            'description' => $description
        ]);
        $this->configuration = new Configuration($this->storageApi, $this->sharedStorage);
        $this->configuration->setWriterId($writerId);

        // Check invitations existence in GD
        $bucketAttributes = $this->configuration->getBucketAttributes();
        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
        $userProjectsInfo = $this->restApi->get('/gdc/projects/' . $bucketAttributes['gd']['pid'] . '/invitations');
        $this->assertArrayHasKey('invitations', $userProjectsInfo, "Response for GoodData API project invitations call should contain 'invitations' key.");
        $this->assertGreaterThanOrEqual(1, $userProjectsInfo['invitations'], "Response for GoodData API project invitations call should return at least one invitation.");
        $user1Invited = false;
        $user2Invited = false;
        foreach ($userProjectsInfo['invitations'] as $p) {
            if (isset($p['invitation']['content']['email']) && $p['invitation']['content']['email'] == $user1) {
                $user1Invited = true;
            }
            if (isset($p['invitation']['content']['email']) && $p['invitation']['content']['email'] == $user2) {
                $user2Invited = true;
            }
        }
        $this->assertTrue($user1Invited, "Response for GoodData API project invitations call should return tested user 1.");
        $this->assertTrue($user2Invited, "Response for GoodData API project invitations call should return tested user 2.");


        /**
         * Test writers configuration
         */
        $responseJson = $this->getWriterApi('/writers');

        $this->assertArrayHasKey('writers', $responseJson, "Response for writer call '/writers' should contain 'writers' key.");
        $this->assertGreaterThanOrEqual(2, $responseJson['writers'], "Response for writer call '/writers' should contain two writers.");
        $this->assertArrayHasKey('id', $responseJson['writers'][0], "Response for writer call '/writers' should contain 'writers..id' key.");
        $this->assertArrayHasKey('bucket', $responseJson['writers'][0], "Response for writer call '/writers' should contain 'writers..bucket' key.");
        $this->assertEquals($this->writerId, $responseJson['writers'][0]['id'], "Response for writer call '/writers' should contain id of created writer.");
        $this->assertEquals($this->bucketId, $responseJson['writers'][0]['bucket'], "Response for writer call '/writers' should contain bucket of created writer.");


        $responseJson = $this->getWriterApi('/writers?writerId=' . $this->writerId);

        $this->assertArrayHasKey('writer', $responseJson, "Response for writer call '/writers?writerId=' should contain 'writer' key.");
        $this->assertArrayHasKey('writer', $responseJson['writer'], "Response for writer call '/writers?writerId=' should contain 'writer.writer' key.");
        $this->assertArrayHasKey('writerId', $responseJson['writer'], "Response for writer call '/writers?writerId=' should contain 'writer.writerId' key.");
        $this->assertEquals($this->writerId, $responseJson['writer']['writerId'], "Response for writer call '/writers?writerId=' should contain id of created writer.");
        $this->assertEquals('gooddata', $responseJson['writer']['writer'], "Response for writer call '/writers?writerId=' should contain name of the writer.");

        $responseJson = $this->getWriterApi('/writers?writerId=' . $writerId);
        $this->assertArrayHasKey('writer', $responseJson, "Response for writer call '/writers?writerId=' should contain 'writer' key.");
        $this->assertArrayHasKey('description', $responseJson['writer'], "Response for writer call '/writers?writerId=' should contain 'writer.description' key.");



        /**
         * Create writer with existing project
         */
        /*$existingPid = $bucketAttributes['gd']['pid'];
        $existingProjectWriterId = self::WRITER_ID_PREFIX . 'exist_' . uniqid();
        $this->processJob('/writers', [
            'writerId' => $existingProjectWriterId,
            'username' => $bucketAttributes['gd']['username'],
            'password' => $bucketAttributes['gd']['password'],
            'pid' => $existingPid
        ]);
        $responseJson = $this->getWriterApi('/writers?writerId=' . $existingProjectWriterId);
        $this->assertArrayHasKey('writer', $responseJson, "Response for writer call '/writers?writerId=' should contain 'writer' key.");
        $this->assertArrayHasKey('gd', $responseJson['writer'], "Response for writer call '/writers?writerId=' should contain 'writer.gd' key.");
        $this->assertArrayHasKey('username', $responseJson['writer']['gd'], "Response for writer call '/writers?writerId=' should contain 'writer.gd.username' key.");
        $this->assertArrayHasKey('password', $responseJson['writer']['gd'], "Response for writer call '/writers?writerId=' should contain 'writer.gd.password' key.");
        $this->assertArrayHasKey('pid', $responseJson['writer']['gd'], "Response for writer call '/writers?writerId=' should contain 'writer.gd.pid' key.");
        $this->assertEquals($bucketAttributes['gd']['pid'], $responseJson['writer']['gd']['pid'], "Writer should have project given as request parameter");

        /** @var Search $jobSearch */
        /*$jobSearch = $this->container->get('gooddata_writer.elasticsearch.search');
        $i = 0;
        do {
            $this->assertLessThanOrEqual(10, $i, 'Waited for getting access to existing project too long');
            sleep($i * 10);
            $jobsFinished = true;
            $jobs = $jobSearch->getJobs(['projectId' => $this->configuration->projectId, 'query' => sprintf('params.writerId:"%s"', $existingProjectWriterId)]);
            foreach ($jobs as $job) {
                if (!Job::isJobFinished($job['status'])) {
                    $this->commandTester->execute([
                        'command' => 'syrup:run-job',
                        'jobId' => $job['id']
                    ]);
                    $jobInfo = $jobSearch->getJob($job['id']);
                    if ($jobInfo['status'] != Job::STATUS_SUCCESS) {
                        $jobsFinished = false;
                    }
                }
            }
            $i++;
        } while (!$jobsFinished);

        $configuration = new Configuration($this->storageApi, $this->sharedStorage);
        $configuration->setWriterId($existingProjectWriterId);
        $bucketAttributes = $configuration->bucketAttributes();
        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
        $projectInfo = $this->restApi->get('/gdc/md/' . $existingPid);
        $this->assertArrayHasKey('about', $projectInfo, "Writer created from existing project should have working credentials to the project");*/


        /**
         * Set configuration
         */
        $this->postWriterApi('/writers/' . $writerId, [
            'attribute' => 1
        ]);
        $writerInfo = $this->getWriterApi('/writers/' . $writerId);
        $this->assertArrayHasKey('writer', $writerInfo, "Writer should have set attribute 'writer'");
        $this->assertArrayHasKey('attribute', $writerInfo['writer'], "Writer should have set attribute 'writer.attribute'");
        $this->assertEquals(1, $writerInfo['writer']['attribute'], "Writer should have set attribute 'writer.attribute' with value '1'");


        /**
         * Delete writer
         */
        $this->processJob('/writers?writerId=' . $this->writerId, [], 'DELETE');
        // Check non-existence of configuration
        $this->assertFalse($this->storageApi->bucketExists($this->bucketId), "Writer configuration should not exist anymore.");
    }
}
