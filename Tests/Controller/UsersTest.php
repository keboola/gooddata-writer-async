<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */

namespace Keboola\GoodDataWriter\Tests\Controller;

class UsersTest extends AbstractControllerTest
{
	public function testUsers()
	{
		/**
		 * Create user
		 */
		$ssoProvider = 'keboola.com';
		$user = $this->_createUser($ssoProvider);

		// Check of GoodData
		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$userInfo = $this->restApi->getUser($user['uid']);
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


		/**
		 * Add user to project
		 */
		$projectsList = $this->configuration->getProjects();
		$this->assertGreaterThanOrEqual(1, $projectsList, "Response for writer call '/projects' should return at least one GoodData project.");
		$project = $projectsList[count($projectsList)-1];

		// Case 1  - User exists
		$this->_processJob('/gooddata-writer/project-users', array(
			'email' => $user['email'],
			'pid' => $project['pid'],
			'role' => 'editor'
		));

		// Check GoodData
		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$userProjectsInfo = $this->restApi->get('/gdc/projects/' . $project['pid'] . '/users');
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
			if (isset($u['email']) && $u['email'] == $user['email']) {
				$userInProject = true;
				break;
			}
		}
		$this->assertTrue($userInProject, "Response for writer call '/project-users' should return tested user.");

		// Case 2 - User exists in other domain
		$this->restApi->login($this->appConfiguration->gd_username, $this->appConfiguration->gd_password);

		$otherUser = null;
		if (defined('WRITER_TEST_OTHER_DOMAIN_USER'))
			$otherUser = WRITER_TEST_OTHER_DOMAIN_USER;

		$this->assertNotEmpty($otherUser, "User from other domain should be configured in tests config file.");

		$otherUserId = $this->restApi->userId($otherUser, $this->appConfiguration->gd_domain);

		$this->assertFalse($otherUserId, "Invited user for writer call '/project-users' should not exist in same domain.");

		$this->_processJob('/gooddata-writer/project-users', array(
			'email' => $otherUser,
			'pid' => $project['pid'],
			'role' => 'editor'
		));

		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		// Check Writer API
		$responseJson = $this->_getWriterApi('/gooddata-writer/project-users?writerId=' . $this->writerId . '&pid=' . $project['pid']);
		$this->assertArrayHasKey('users', $responseJson, "Response for writer call '/project-users' should contain 'users' key.");
		$this->greaterThanOrEqual(2, $responseJson['users'], "Response for writer call '/project-users' should return at least one result.");
		$userInvited = false;
		foreach ($responseJson['users'] as $u) {
			if (isset($u['email']) == $otherUser) {
				$userInvited = true;
				break;
			}
		}
		$this->assertTrue($userInvited, "Response for writer call '/project-users' should return invited user.");

		// Check GoodData
		$userInvitationsInfo = $this->restApi->get('/gdc/projects/' . $project['pid'] . '/invitations');
		$this->assertArrayHasKey('invitations', $userInvitationsInfo, "Response for GoodData API project invitations call should contain 'invitations' key.");
		$this->assertCount(1, $userInvitationsInfo['invitations'], "Response for GoodData API project users call should return three users.");
		$userInvited = false;
		foreach ($userInvitationsInfo['invitations'] as $p) {
			if (isset($p['invitation']['content']['email']) && $p['invitation']['content']['email'] == $otherUser) {
				if (isset($p['invitation']['content']['status']) && $p['invitation']['content']['status'] == 'WAITING') {
					$userInvited = true;
					break;
				}
			}
		}
		$this->assertTrue($userInvited, "Response for GoodData API project users call should return invitation for user.");

		// Case 3  - User does not exists
		$otherUser = 'testcreate' . $user['email'];
		$this->_processJob('/gooddata-writer/project-users', array(
			'email' => $otherUser,
			'pid' => $project['pid'],
			'role' => 'editor',
			'createUser' => 1,
		));

		// Check GoodData
		$userProjectsInfo = $this->restApi->get('/gdc/projects/' . $project['pid'] . '/users');
		$this->assertArrayHasKey('users', $userProjectsInfo, "Response for GoodData API project users call should contain 'users' key.");
		$this->assertCount(4, $userProjectsInfo['users'], "Response for GoodData API project users call should return three users.");
		$userInProject = false;
		foreach ($userProjectsInfo['users'] as $p) {
			if (isset($p['user']['content']['email']) && $p['user']['content']['email'] == $otherUser) {
				$userInProject = true;
				break;
			}
		}
		$this->assertTrue($userInProject, "Response for GoodData API project users call should return tested user.");


		// Check Writer API
		$responseJson = $this->_getWriterApi('/gooddata-writer/project-users?writerId=' . $this->writerId . '&pid=' . $project['pid']);
		$this->assertArrayHasKey('users', $responseJson, "Response for writer call '/project-users' should contain 'users' key.");
		$this->greaterThanOrEqual(3, $responseJson['users'], "Response for writer call '/project-users' should return at least one result.");
		$userInProject = false;
		foreach ($responseJson['users'] as $u) {
			if (isset($u['email']) == $otherUser) {
				$userInProject = true;
				break;
			}
		}
		$this->assertTrue($userInProject, "Response for writer call '/project-users' should return tested user.");


		/**
		 * Remove user from project
		 */

		// Case 1  - User exists
		$this->_processJob('/gooddata-writer/project-users', array(
			'email' => $user['email'],
			'pid' => $project['pid'],
			'role' => 'editor'
		));

		// Check GoodData
		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		$userProjectsInfo = $this->restApi->get('/gdc/projects/' . $project['pid'] . '/users');
		$this->assertArrayHasKey('users', $userProjectsInfo, "Response for GoodData API project users call should contain 'users' key.");
		$this->assertCount(4, $userProjectsInfo['users'], "Response for GoodData API project users call should return four users.");
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
			if (isset($u['email']) && $u['email'] == $user['email']) {
				$userInProject = true;
				break;
			}
		}
		$this->assertTrue($userInProject, "Response for writer call '/project-users' should return tested user.");

		$params = array(
			'writerId=' . $this->writerId,
			'pid=' . $project['pid'],
			'email=' . $user['email'],
		);

		$this->_processJob('/gooddata-writer/project-users?' . implode('&', $params), array(), 'DELETE');

		// Check GoodData
		$userProjectsInfo = $this->restApi->get('/gdc/projects/' . $project['pid'] . '/users');
		$this->assertArrayHasKey('users', $userProjectsInfo, "Response for GoodData API project users call should contain 'users' key.");
		$this->assertCount(4, $userProjectsInfo['users'], "Response for GoodData API project users call should return four users.");
		$userInProject = false;
		foreach ($userProjectsInfo['users'] as $p) {
			if (isset($p['user']['content']['email']) && $p['user']['content']['email'] == $user['email']) {
				$userInProject = true;

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
		$this->assertCount(2, $responseJson['users'], "Response for writer call '/project-users' should return two results.");
		$userInProject = false;
		foreach ($responseJson['users'] as $u) {
			if (isset($u['email']) && $u['email'] == $user['email']) {
				$userInProject = true;
				break;
			}
		}
		$this->assertFalse($userInProject, "Response for writer call '/project-users' should not return tested user.");

		// Case 2 - User exists in other domain
		$this->restApi->login($this->appConfiguration->gd_username, $this->appConfiguration->gd_password);

		$otherUser = null;
		if (defined('WRITER_TEST_OTHER_DOMAIN_USER'))
			$otherUser = WRITER_TEST_OTHER_DOMAIN_USER;

		$this->assertNotEmpty($otherUser, "User from other domain should be configured in tests config file.");

		$otherUserId = $this->restApi->userId($otherUser, $this->appConfiguration->gd_domain);

		$this->assertFalse($otherUserId, "Invited user for writer call '/project-users' should not exist in same domain.");

		$this->_processJob('/gooddata-writer/project-users', array(
			'email' => $otherUser,
			'pid' => $project['pid'],
			'role' => 'editor'
		));

		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		// Check Writer API
		$responseJson = $this->_getWriterApi('/gooddata-writer/project-users?writerId=' . $this->writerId . '&pid=' . $project['pid']);
		$this->assertArrayHasKey('users', $responseJson, "Response for writer call '/project-users' should contain 'users' key.");
		$this->greaterThanOrEqual(2, $responseJson['users'], "Response for writer call '/project-users' should return at least one result.");
		$userInvited = false;
		foreach ($responseJson['users'] as $u) {
			if (isset($u['email']) == $otherUser) {
				$userInvited = true;
				break;
			}
		}
		$this->assertTrue($userInvited, "Response for writer call '/project-users' should return invited user.");

		// Check GoodData
		$userInvitationsInfo = $this->restApi->get('/gdc/projects/' . $project['pid'] . '/invitations');
		$this->assertArrayHasKey('invitations', $userInvitationsInfo, "Response for GoodData API project invitations call should contain 'invitations' key.");
		$this->assertCount(1, $userInvitationsInfo['invitations'], "Response for GoodData API project users call should return three users.");

		$userInvited = false;
		foreach ($userInvitationsInfo['invitations'] as $p) {
			if (isset($p['invitation']['content']['email']) && $p['invitation']['content']['email'] == $otherUser) {
				if (isset($p['invitation']['content']['status']) && $p['invitation']['content']['status'] == 'WAITING') {
					$userInvited = true;
					break;
				}
			}
		}
		$this->assertTrue($userInvited, "Response for GoodData API project users call should return invitation for user.");

		$params = array(
			'writerId=' . $this->writerId,
			'pid=' . $project['pid'],
			'email=' . $otherUser,
		);

		$this->_processJob('/gooddata-writer/project-users?' . implode('&', $params), array(), 'DELETE');

		// Check GoodData
		$userProjectsInfo = $this->restApi->get('/gdc/projects/' . $project['pid'] . '/users');
		$this->assertArrayHasKey('users', $userProjectsInfo, "Response for GoodData API project users call should contain 'users' key.");
		$this->assertCount(4, $userProjectsInfo['users'], "Response for GoodData API project users call should return four users.");
		$userInProject = false;

		foreach ($userProjectsInfo['users'] as $p) {
			if (isset($p['user']['content']['email']) && $p['user']['content']['email'] == $otherUser) {
				$userInProject = true;

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
		$this->assertCount(1, $responseJson['users'], "Response for writer call '/project-users' should return one result.");
		$userInProject = false;

		foreach ($responseJson['users'] as $u) {
			if (isset($u['email']) && $u['email'] == $otherUser) {
				$userInProject = true;
				break;
			}
		}
		$this->assertFalse($userInProject, "Response for writer call '/project-users' should not return tested user.");

		// Check GoodData
		$userInvitationsInfo = $this->restApi->get('/gdc/projects/' . $project['pid'] . '/invitations');
		$this->assertArrayHasKey('invitations', $userInvitationsInfo, "Response for GoodData API project invitations call should contain 'invitations' key.");
		$this->assertCount(1, $userInvitationsInfo['invitations'], "Response for GoodData API project users call should return three users.");
		$userInvited = false;
		foreach ($userInvitationsInfo['invitations'] as $p) {
			if (isset($p['invitation']['content']['email']) && $p['invitation']['content']['email'] == $otherUser) {
				$userInvited = true;
				if (isset($p['invitation']['content']['status']) && $p['invitation']['content']['status'] == 'CANCELED') {
					$userInvited = false;
					break;
				}
			}
		}
		$this->assertFalse($userInvited, "Response for GoodData API project users call should return invitation for user.");

		// Case 3  - User does not exists
		$otherUser = 'testcreate' . $user['email'];
		$this->_processJob('/gooddata-writer/project-users', array(
			'email' => $otherUser,
			'pid' => $project['pid'],
			'role' => 'editor',
			'createUser' => 1,
		));

		// Check GoodData
		$userProjectsInfo = $this->restApi->get('/gdc/projects/' . $project['pid'] . '/users');
		$this->assertArrayHasKey('users', $userProjectsInfo, "Response for GoodData API project users call should contain 'users' key.");
		$this->assertCount(4, $userProjectsInfo['users'], "Response for GoodData API project users call should return three users.");
		$userInProject = false;
		foreach ($userProjectsInfo['users'] as $p) {
			if (isset($p['user']['content']['email']) && $p['user']['content']['email'] == $otherUser) {
				$userInProject = true;
				break;
			}
		}
		$this->assertTrue($userInProject, "Response for GoodData API project users call should return tested user.");


		// Check Writer API
		$responseJson = $this->_getWriterApi('/gooddata-writer/project-users?writerId=' . $this->writerId . '&pid=' . $project['pid']);
		$this->assertArrayHasKey('users', $responseJson, "Response for writer call '/project-users' should contain 'users' key.");
		$this->greaterThanOrEqual(3, $responseJson['users'], "Response for writer call '/project-users' should return at least one result.");
		$userInProject = false;
		foreach ($responseJson['users'] as $u) {
			if (isset($u['email']) == $otherUser) {
				$userInProject = true;
				break;
			}
		}
		$this->assertTrue($userInProject, "Response for writer call '/project-users' should return tested user.");

		$params = array(
			'writerId=' . $this->writerId,
			'pid=' . $project['pid'],
			'email=' . $otherUser,
		);

		$this->_processJob('/gooddata-writer/project-users?' . implode('&', $params), array(), 'DELETE');

		// Check GoodData
		$userProjectsInfo = $this->restApi->get('/gdc/projects/' . $project['pid'] . '/users');
		$this->assertArrayHasKey('users', $userProjectsInfo, "Response for GoodData API project users call should contain 'users' key.");
		$this->assertCount(4, $userProjectsInfo['users'], "Response for GoodData API project users call should return three users.");
		$userInProject = false;

		foreach ($userProjectsInfo['users'] as $p) {
			if (isset($p['user']['content']['email']) && $p['user']['content']['email'] == $otherUser) {
				$userInProject = true;

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
		$this->assertCount(1, $responseJson['users'], "Response for writer call '/project-users' should return one result.");
		$userInProject = false;

		foreach ($responseJson['users'] as $u) {
			if (isset($u['email']) && $u['email'] == $otherUser) {
				$userInProject = true;
				break;
			}
		}
		$this->assertFalse($userInProject, "Response for writer call '/project-users' should not return tested user.");

		// Check GoodData
		$userInvitationsInfo = $this->restApi->get('/gdc/projects/' . $project['pid'] . '/invitations');
		$this->assertArrayHasKey('invitations', $userInvitationsInfo, "Response for GoodData API project invitations call should contain 'invitations' key.");
		$this->assertCount(1, $userInvitationsInfo['invitations'], "Response for GoodData API project users call should return three users.");
		$userInvited = false;
		foreach ($userInvitationsInfo['invitations'] as $p) {
			if (isset($p['invitation']['content']['email']) && $p['invitation']['content']['email'] == $otherUser) {
				$userInvited = true;
				if (isset($p['invitation']['content']['status']) && $p['invitation']['content']['status'] == 'CANCELED') {
					$userInvited = false;
					break;
				}
			}
		}
		$this->assertFalse($userInvited, "Response for GoodData API project users call should return invitation for user.");


		/**
		 * Test SSO
		 */
		$responseJson = $this->_getWriterApi(
			'/gooddata-writer/sso'
			. '?writerId=' . $this->writerId
			. '&pid=' . $project['pid']
			. '&email=' . $user['email']
		);

		$this->assertArrayHasKey('ssoLink', $responseJson, "No ssoLink in response");
		$this->assertNotNull($responseJson['ssoLink'], "SSO Link is NULL");
	}
}
