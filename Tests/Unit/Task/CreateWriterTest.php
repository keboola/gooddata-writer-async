<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\GoodDataWriter\Tests\Unit\Task;

use Keboola\GoodDataWriter\Task\CreateWriter;
use Keboola\Syrup\Exception\UserException;

class CreateWriterTest extends AbstractTaskTest
{

    public function testCreateWriterPreparation()
    {
        /** @var CreateWriter $task */
        $task = $this->taskFactory->create('createWriter');

        // Writer name should not contain dots and dashes
        try {
            $task->prepare(['writerId' => 'dfs-dsf.fds'], $this->restApi);
            $this->fail();
        } catch (UserException $e) {
        }

        // Writer name should not be longer then fifty chars
        try {
            $task->prepare(['writerId' => 'dfsdsffdsdfsdsffdsdfsdsffdsdfsdsffdsdfsdsffdsdfsdsffds'], $this->restApi);
            $this->fail();
        } catch (UserException $e) {
        }

        $params = $task->prepare(['writerId' => $this->configuration->writerId, 'users' => 'u1,u2,u3']);
        $this->assertArrayHasKey('accessToken', $params);
        $this->assertArrayHasKey('projectName', $params);
        $this->assertArrayHasKey('users', $params);
        $this->assertCount(3, $params['users']);
    }

    public function testCreateWriterPreparationWithExistingProject()
    {
        /** @var CreateWriter $task */
        $task = $this->taskFactory->create('createWriter');

        // Test existing project and user
        $uniqId = uniqId();
        $existingUserName = $uniqId . '@test.keboola.com';
        $existingPassword = $uniqId;
        $this->restApi->login(GW_GD_DOMAIN_USER, GW_GD_DOMAIN_PASSWORD);
        $existingPid = $this->restApi->createProject('[Test]', $this->gdConfig['access_token']);
        $existingUid = $this->restApi->createUser(GW_GD_DOMAIN_NAME, $existingUserName, $existingPassword, 'Test', $uniqId, GW_GD_SSO_PROVIDER);
        $this->restApi->addUserToProject($existingUid, $existingPid);

        $task->prepare(['writerId' => $this->configuration->writerId, 'username' => $existingUserName, 'pid' => $existingPid, 'password' => $existingPassword]);
    }

    public function testCreateWriter()
    {
        $job = $this->createJob();
        /** @var CreateWriter $task */
        $task = $this->taskFactory->create('createWriter');

        $inputParams = ['writerId' => $this->configuration->writerId];
        $preparedParams = $task->prepare($inputParams, $this->restApi);

        $this->assertArrayHasKey('accessToken', $preparedParams);
        $this->assertArrayHasKey('projectName', $preparedParams);

        $result = $task->run($job, 0, $preparedParams);

        $this->assertArrayHasKey('uid', $result);
        $this->assertArrayHasKey('pid', $result);


        // Check writer configuration
        $bucketAttributes = false;
        try {
            $bucketAttributes = $this->configuration->bucketAttributes();
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
    }

    public function testCreateWriterWithInvitation()
    {
        $job = $this->createJob();
        /** @var CreateWriter $task */
        $task = $this->taskFactory->create('createWriter');

        $uniqId = uniqId();
        $user1 = 'user1' . $uniqId . '@test.keboola.com';
        $user2 = 'user2' . $uniqId . '@test.keboola.com';
        $description = uniqId();

        $inputParams = ['writerId' => $this->configuration->writerId, 'users' => $user1 . ',' . $user2, 'description' => $description];
        $preparedParams = $task->prepare($inputParams, $this->restApi);

        $this->assertArrayHasKey('accessToken', $preparedParams);
        $this->assertArrayHasKey('projectName', $preparedParams);
        $this->assertArrayHasKey('description', $preparedParams);
        $this->assertArrayHasKey('users', $preparedParams);
        $this->assertEquals([$user1, $user2], $preparedParams['users']);

        $result = $task->run($job, 0, $preparedParams);

        $this->assertArrayHasKey('uid', $result);
        $this->assertArrayHasKey('pid', $result);
    }

    public function testCreateWriterWithExistingProject()
    {
        $job = $this->createJob();
        /** @var CreateWriter $task */
        $task = $this->taskFactory->create('createWriter');

        $uniqId = uniqId();
        $existingUserName = 'user1' . $uniqId . '@test.keboola.com';

        // Prepare existing project and user
        $this->restApi->login(GW_GD_DOMAIN_USER, GW_GD_DOMAIN_PASSWORD);
        $existingPid = $this->restApi->createProject('[Test]', $this->gdConfig['access_token']);
        $existingUid = $this->restApi->createUser(GW_GD_DOMAIN_NAME, $existingUserName, $uniqId, 'Test', $uniqId, GW_GD_SSO_PROVIDER);
        $this->restApi->addUserToProject($existingUid, $existingPid);

        $inputParams = ['writerId' => $this->configuration->writerId, 'username' => $existingUserName, 'password' => $uniqId, 'pid' => $existingPid];
        $preparedParams = $task->prepare($inputParams, $this->restApi);

        $this->assertArrayHasKey('username', $preparedParams);
        $this->assertArrayHasKey('password', $preparedParams);
        $this->assertArrayHasKey('pid', $preparedParams);

        $result = $task->run($job, 0, $preparedParams);

        $this->assertArrayHasKey('uid', $result);
        $this->assertArrayHasKey('pid', $result);
        $this->assertEquals($existingPid, $result['pid']);

        //@TODO Wait for invitation to project
    }
}
