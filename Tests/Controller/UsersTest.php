<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */

namespace Keboola\GoodDataWriter\Tests\Controller;

use Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\StorageApi\Table as StorageApiTable;

class UsersTest extends AbstractControllerTest
{
	public function testCreateUser()
	{
		$user = $this->_createUser();

		// Check of GoodData
		self::$restApi->setCredentials(self::$configuration->bucketInfo['gd']['username'], self::$configuration->bucketInfo['gd']['password']);
		$userInfo = self::$restApi->getUser($user['uid']);
		$this->assertArrayHasKey('accountSetting', $userInfo, "Response for GoodData API user call should contain 'accountSetting' key.");


		// Check of Writer API
		$responseJson = $this->_getWriterApi('/gooddata-writer/users?writerId=' . $this->writerId);
		$this->assertArrayHasKey('users', $responseJson, "Response for writer call '/users' should contain 'users' key.");
		$this->assertCount(2, $responseJson['users'], "Response for writer call '/users' should return two users.");
		$userFound = false;
		foreach ($responseJson['users'] as $u) {
			if ($u['uid'] == $user['uid']) {
				$userFound = true;
			}
		}
		$this->assertTrue($userFound, "Response for writer call '/users' should return tested user.");

		$responseJson = $this->_getWriterApi('/gooddata-writer/users?writerId=' . $this->writerId . '&userEmail=' . $user['email']);
		$this->assertArrayHasKey('user', $responseJson, "Response for writer call '/users' with 'userEmail' filter should contain 'user' key.");
		$this->assertNotNull($responseJson['user'], "Response for writer call '/users' with 'userEmail' filter should return one user data.");
		$this->assertEquals($user['email'], $responseJson['user']['email'], "Response for writer call '/users' with 'userEmail' filter should return user data of test user.");
	}

	public function testAddUserToProject()
	{
		$user = $this->_createUser();

		$projectsList = self::$configuration->getProjects();
		$this->assertGreaterThanOrEqual(1, $projectsList, "Response for writer call '/projects' should return at least one GoodData project.");
		$project = $projectsList[count($projectsList)-1];


		$this->_processJob('/gooddata-writer/project-users', array(
			'email' => $user['email'],
			'pid' => $project['pid'],
			'role' => 'editor'
		));

		// Check GoodData
		self::$restApi->setCredentials(self::$configuration->bucketInfo['gd']['username'], self::$configuration->bucketInfo['gd']['password']);
		$userProjectsInfo = self::$restApi->get('/gdc/projects/' . $project['pid'] . '/users');
		$this->assertArrayHasKey('users', $userProjectsInfo, "Response for GoodData API project users call should contain 'users' key.");
		$this->assertCount(3, $userProjectsInfo['users'], "Response for GoodData API project users call should return three users.");
		$userInProject = false;
		foreach ($userProjectsInfo['users'] as $p) {
			if (isset($p['user']['content']['email']) && $p['user']['content']['email'] == $user['email']) {
				$userInProject = true;
				break;
			}
		}
		$this->assertTrue($userInProject, "Response for GoodData API project users call should return tested user.");


		// Check Writer API
		$responseJson = $this->_getWriterApi('/gooddata-writer/project-users?writerId=' . $this->writerId . '&pid=' . $project['pid']);
		$this->assertArrayHasKey('users', $responseJson, "Response for writer call '/project-users' should contain 'users' key.");
		$this->greaterThanOrEqual(1, $responseJson['users'], "Response for writer call '/project-users' should return at least one result.");
		$userInProject = false;
		foreach ($responseJson['users'] as $u) {
			if (isset($u['email']) == $user['email']) {
				$userInProject = true;
				break;
			}
		}
		$this->assertTrue($userInProject, "Response for writer call '/project-users' should return tested user.");
	}

	public function testInviteUserToProject()
	{
		$user = $this->_createUser();

		$projectsList = self::$configuration->getProjects();
		$this->assertGreaterThanOrEqual(1, $projectsList, "Response for writer call '/projects' should return at least one GoodData project.");
		$project = $projectsList[count($projectsList)-1];


		$this->_processJob('/gooddata-writer/project-invitations', array(
			'email' => $user['email'],
			'pid' => $project['pid'],
			'role' => 'editor'
		));

		// Check GoodData
		self::$restApi->setCredentials(self::$configuration->bucketInfo['gd']['username'], self::$configuration->bucketInfo['gd']['password']);
		$userProjectsInfo = self::$restApi->get('/gdc/projects/' . $project['pid'] . '/invitations');
		$this->assertArrayHasKey('invitations', $userProjectsInfo, "Response for GoodData API project invitations call should contain 'invitations' key.");
		$this->assertGreaterThanOrEqual(1, $userProjectsInfo['invitations'], "Response for GoodData API project invitations call should return at least one invitation.");
		$userInvited = false;
		foreach ($userProjectsInfo['invitations'] as $p) {
			if (isset($p['invitation']['content']['email']) && $p['invitation']['content']['email'] == $user['email']) {
				$userInvited = true;
				break;
			}
		}
		$this->assertTrue($userInvited, "Response for GoodData API project invitations call should return tested user.");
	}

	public function testRemoveUserFromProject()
	{
		$user = $this->_createUser();

		$projectsList = self::$configuration->getProjects();
		$this->assertGreaterThanOrEqual(1, $projectsList, "Response for writer call '/projects' should return at least one GoodData project.");
		$project = $projectsList[count($projectsList)-1];


		$this->_processJob('/gooddata-writer/project-users', array(
			'email' => $user['email'],
			'pid' => $project['pid'],
			'role' => 'editor'
		));

		// Check GoodData
		self::$restApi->setCredentials(self::$configuration->bucketInfo['gd']['username'], self::$configuration->bucketInfo['gd']['password']);
		$userProjectsInfo = self::$restApi->get('/gdc/projects/' . $project['pid'] . '/users');
		$this->assertArrayHasKey('users', $userProjectsInfo, "Response for GoodData API project users call should contain 'users' key.");
		$this->assertCount(3, $userProjectsInfo['users'], "Response for GoodData API project users call should return three users.");
		$userInProject = false;
		foreach ($userProjectsInfo['users'] as $p) {
			if (isset($p['user']['content']['email']) && $p['user']['content']['email'] == $user['email']) {
				$userInProject = true;
				break;
			}
		}
		$this->assertTrue($userInProject, "Response for GoodData API project users call should return tested user.");

		// Check Writer API
		$responseJson = $this->_getWriterApi('/gooddata-writer/project-users?writerId=' . $this->writerId . '&pid=' . $project['pid']);
		$this->assertArrayHasKey('users', $responseJson, "Response for writer call '/project-users' should contain 'users' key.");
		$this->greaterThanOrEqual(1, $responseJson['users'], "Response for writer call '/project-users' should return at least one result.");
		$userInProject = false;
		foreach ($responseJson['users'] as $u) {
			if (isset($u['email']) == $user['email']) {
				$userInProject = true;
				break;
			}
		}
		$this->assertTrue($userInProject, "Response for writer call '/project-users' should return tested user.");

		$params = array(
			'writerId=' . $this->writerId,
			'pid=' . $project['pid'],
			'email=' . $user['email'],
			'dev=' . 1,
		);

		$this->_processJob('/gooddata-writer/project-users?' . implode('&', $params), array(), 'DELETE');

		// Check GoodData
		self::$restApi->setCredentials(self::$configuration->bucketInfo['gd']['username'], self::$configuration->bucketInfo['gd']['password']);
		$userProjectsInfo = self::$restApi->get('/gdc/projects/' . $project['pid'] . '/users');
		$this->assertArrayHasKey('users', $userProjectsInfo, "Response for GoodData API project users call should contain 'users' key.");
		$this->assertCount(3, $userProjectsInfo['users'], "Response for GoodData API project users call should return three users.");
		$userInProject = false;

		foreach ($userProjectsInfo['users'] as $p) {
			if (isset($p['user']['content']['email']) && $p['user']['content']['email'] == $user['email']) {
				$userInProject = true;
print_r($p);
				if (isset($p['user']['content']['status']) && $p['user']['content']['status'] == 'DISABLED') {
					$userInProject = false;
				}

				break;
			}
		}
		$this->assertFalse($userInProject, "Response for GoodData API project users call should return disabled tested user.");

		// Check Writer API
		$responseJson = $this->_getWriterApi('/gooddata-writer/project-users?writerId=' . $this->writerId . '&pid=' . $project['pid']);
		$this->assertArrayHasKey('users', $responseJson, "Response for writer call '/project-users' should contain 'users' key.");
		$this->greaterThanOrEqual(1, $responseJson['users'], "Response for writer call '/project-users' should return at least one result.");
		$userInProject = false;
		print_r($responseJson['users']);
		foreach ($responseJson['users'] as $u) {
			if (isset($u['email']) == $user['email']) {
				$userInProject = true;
				break;
			}
		}
//		$this->assertTrue($userInProject, "Response for writer call '/project-users' should return tested user.");
	}

	public function testSso()
	{
		$email = 'test' . time() . uniqid() . '@test.keboola.com';
		$password = md5(uniqid());
		$firstName = 'Test';
		$lastName = 'KBC';
		$role = 'editor';

		$projectsList = self::$configuration->getProjects();
		$project = $projectsList[count($projectsList)-1];

		$responseJson = $this->_getWriterApi(
			'/gooddata-writer/sso'
			. '?writerId=' . $this->writerId
			. '&pid=' . $project['pid']
			. '&email=' . $email
			. '&role=' . $role
			. '&firstName=' . $firstName
			. '&lastName=' . $lastName
			. '&password=' . $password
			. '&createUser=1'
		);

		$this->assertArrayHasKey('ssoLink', $responseJson, "No ssoLink in response");
		$this->assertNotNull($responseJson['ssoLink'], "SSO Link is NULL");
	}
}
