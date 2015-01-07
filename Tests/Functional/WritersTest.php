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

		$jobInfo = array(
			'id' => uniqid(),
			'batchId' => uniqid(),
			'projectId' => rand(1, 128),
			'writerId' => $writerId,
			'token' => STORAGE_API_TOKEN,
			'tokenId' => rand(1, 128),
			'tokenDesc' => uniqid(),
			'createdTime' => date('c'),
			'command' => 'createWriter',
			'parameters' => $preparedParams
		);
		$result = $job->run($jobInfo, $preparedParams, $this->restApi);

		$this->assertArrayHasKey('uid', $result);
		$this->assertArrayHasKey('pid', $result);


		// Check writer configuration
		$validConfiguration = true;
		$bucketAttributes = $this->configuration->bucketAttributes();
		try {
			$this->configuration->checkBucketAttributes();
		} catch (WrongConfigurationException $e) {
			$validConfiguration = false;
		}
		$this->assertTrue($validConfiguration, "Writer configuration is not valid.");

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

		$jobInfo = array(
			'id' => uniqid(),
			'batchId' => uniqid(),
			'projectId' => rand(1, 128),
			'writerId' => $writerId,
			'token' => STORAGE_API_TOKEN,
			'tokenId' => rand(1, 128),
			'tokenDesc' => uniqid(),
			'createdTime' => date('c'),
			'command' => 'createWriter',
			'parameters' => $preparedParams
		);
		$result = $job->run($jobInfo, $preparedParams, $this->restApi);

		$this->assertArrayHasKey('uid', $result);
		$this->assertArrayHasKey('pid', $result);


		// Check invitations existence in GD
		$bucketAttributes = $this->configuration->bucketAttributes();print_r($bucketAttributes);
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$userProjectsInfo = $this->restApi->get('/gdc/projects/' . $bucketAttributes['gd']['pid'] . '/invitations');print_r($userProjectsInfo);
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
	}

	public function testCreateWriterWithExistingProject()
	{
		$uniqId = uniqId();
		$writerId = 'existing_' . $uniqId;
		$existingUserName = 'user1' . $uniqId . '@test.keboola.com';

		/** @var CreateWriter $job */
		$job = $this->jobFactory->getJobClass('createWriter');

		// Prepare existing project and user
		$this->restApi->login(GD_DOMAIN_USER, GD_DOMAIN_PASSWORD);
		$existingPid = $this->restApi->createProject('[Test]', $this->gdConfig['access_token']);
		$existingUid = $this->restApi->createUser(GD_DOMAIN_NAME, $existingUserName, $uniqId, 'Test', $uniqId, GD_SSO_PROVIDER);
		$this->restApi->addUserToProject($existingUid, $existingPid);

		$inputParams = array('writerId' => $writerId, 'username' => $existingUserName, 'password' => $uniqId, 'pid' => $existingPid);
		$preparedParams = $job->prepare($inputParams, $this->restApi);
print_r($preparedParams);
		$this->assertArrayHasKey('username', $preparedParams);
		$this->assertArrayHasKey('password', $preparedParams);
		$this->assertArrayHasKey('pid', $preparedParams);

		$jobInfo = array(
			'id' => uniqid(),
			'batchId' => uniqid(),
			'projectId' => rand(1, 128),
			'writerId' => $writerId,
			'token' => STORAGE_API_TOKEN,
			'tokenId' => rand(1, 128),
			'tokenDesc' => uniqid(),
			'createdTime' => date('c'),
			'command' => 'createWriter',
			'parameters' => $preparedParams
		);
		$result = $job->run($jobInfo, $preparedParams, $this->restApi);
print_r($result);die();
		$this->assertArrayHasKey('uid', $result);
		$this->assertArrayHasKey('pid', $result);


		$this->processJob('/writers', array(
			'writerId' => $existingProjectWriterId,
			'username' => $bucketAttributes['gd']['username'],
			'password' => $bucketAttributes['gd']['password'],
			'pid' => $existingPid
		));
		$responseJson = $this->getWriterApi('/writers?writerId=' . $existingProjectWriterId);
		$this->assertArrayHasKey('writer', $responseJson, "Response for writer call '/writers?writerId=' should contain 'writer' key.");
		$this->assertArrayHasKey('gd', $responseJson['writer'], "Response for writer call '/writers?writerId=' should contain 'writer.gd' key.");
		$this->assertArrayHasKey('username', $responseJson['writer']['gd'], "Response for writer call '/writers?writerId=' should contain 'writer.gd.username' key.");
		$this->assertArrayHasKey('password', $responseJson['writer']['gd'], "Response for writer call '/writers?writerId=' should contain 'writer.gd.password' key.");
		$this->assertArrayHasKey('pid', $responseJson['writer']['gd'], "Response for writer call '/writers?writerId=' should contain 'writer.gd.pid' key.");
		$this->assertEquals($bucketAttributes['gd']['pid'], $responseJson['writer']['gd']['pid'], "Writer should have project given as request parameter");

		$i = 0;
		do {
			$this->assertLessThanOrEqual(10, $i, 'Waited for getting access to existing project too long');
			sleep ($i * 10);
			$jobsFinished = true;
			$jobs = $this->getWriterApi('/jobs?writerId=' . $existingProjectWriterId);
			foreach($jobs['jobs'] as $job) {
				if (!SharedStorage::isJobFinished($job['status'])) {
					$this->commandTester->execute(array(
						'command' => 'gooddata-writer:execute-batch',
						'batchId' => $job['batchId']
					));
					$jobInfo = $this->getWriterApi('/jobs?writerId=' . $existingProjectWriterId . '&jobId=' . $job['id']);
					if ($jobInfo['status'] != SharedStorage::JOB_STATUS_SUCCESS)
						$jobsFinished = false;
				}
			}
			$i++;
		} while (!$jobsFinished);

		$configuration = new Configuration($this->storageApi, $this->sharedStorage);
		$configuration->setWriterId($existingProjectWriterId);
		$bucketAttributes = $configuration->bucketAttributes();
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$projectInfo = $this->restApi->get('/gdc/md/' . $existingPid);
		$this->assertArrayHasKey('about', $projectInfo, "Writer created from existing project should have working credentials to the project");
	}

}
