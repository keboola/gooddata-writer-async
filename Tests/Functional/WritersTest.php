<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\GoodDataWriter\Tests\Functional;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\Job\CreateWriter;

class WritersTest extends AbstractTest
{

    public function testCreateWriter()
    {
        /** @var CreateWriter $job */
        $job = $this->jobFactory->getJobClass('createWriter');

        $writerId = uniqid();
        $inputParams = array('writerId' => $writerId);
        $preparedParams = $job->prepare($inputParams, $this->restApi);

        $this->assertArrayHasKey('accessToken', $preparedParams);
        $this->assertArrayHasKey('projectName', $preparedParams);

        $jobInfo = $this->prepareJobInfo($writerId, 'createWriter', $preparedParams);
        $result = $job->run($jobInfo, $preparedParams, $this->restApi);

        $this->assertArrayHasKey('uid', $result);
        $this->assertArrayHasKey('pid', $result);


        // Check writer configuration
        $bucketAttributes = false;
        try {
            $bucketAttributes = $this->configuration->bucketAttributes();
        } catch (WrongConfigurationException $e) {
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
        $uniqId = uniqId();
        $writerId = 'invit_' . $uniqId;
        $user1 = 'user1' . $uniqId . '@test.keboola.com';
        $user2 = 'user2' . $uniqId . '@test.keboola.com';
        $description = uniqId();

        /** @var CreateWriter $job */
        $job = $this->jobFactory->getJobClass('createWriter');

        $inputParams = array('writerId' => $writerId, 'users' => $user1 . ',' . $user2, 'description' => $description);
        $preparedParams = $job->prepare($inputParams, $this->restApi);

        $this->assertArrayHasKey('accessToken', $preparedParams);
        $this->assertArrayHasKey('projectName', $preparedParams);
        $this->assertArrayHasKey('description', $preparedParams);
        $this->assertArrayHasKey('users', $preparedParams);
        $this->assertEquals(array($user1, $user2), $preparedParams['users']);

        $jobInfo = $this->prepareJobInfo($writerId, 'createWriter', $preparedParams);
        $result = $job->run($jobInfo, $preparedParams, $this->restApi);

        $this->assertArrayHasKey('uid', $result);
        $this->assertArrayHasKey('pid', $result);
    }

    public function testCreateWriterWithExistingProject()
    {
        $uniqId = uniqId();
        $writerId = 'existing_' . $uniqId;
        $existingUserName = 'user1' . $uniqId . '@test.keboola.com';

        $this->configuration->writerId = $writerId;

        /** @var CreateWriter $job */
        $job = $this->jobFactory->getJobClass('createWriter');

        // Prepare existing project and user
        $this->restApi->login(GD_DOMAIN_USER, GD_DOMAIN_PASSWORD);
        $existingPid = $this->restApi->createProject('[Test]', $this->gdConfig['access_token']);
        $existingUid = $this->restApi->createUser(GD_DOMAIN_NAME, $existingUserName, $uniqId, 'Test', $uniqId, GD_SSO_PROVIDER);
        $this->restApi->addUserToProject($existingUid, $existingPid);

        $inputParams = array('writerId' => $writerId, 'username' => $existingUserName, 'password' => $uniqId, 'pid' => $existingPid);
        $preparedParams = $job->prepare($inputParams, $this->restApi);

        $this->assertArrayHasKey('username', $preparedParams);
        $this->assertArrayHasKey('password', $preparedParams);
        $this->assertArrayHasKey('pid', $preparedParams);

        $jobInfo = $this->prepareJobInfo($writerId, 'createWriter', $preparedParams);
        $result = $job->run($jobInfo, $preparedParams, $this->restApi);

        $this->assertArrayHasKey('uid', $result);
        $this->assertArrayHasKey('pid', $result);
        $this->assertEquals($existingPid, $result['pid']);

        //@TODO Wait for invitation to project
    }
}
