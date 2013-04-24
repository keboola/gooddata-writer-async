<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Bundle\FrameworkBundle\Client;
use Symfony\Component\Console\Tester\CommandTester;
use Keboola\GoodDataWriter\Command\RunJobCommand;
use Keboola\GoodDataWriter\Writer\Configuration;

class WriterTest extends WebTestCase
{
	const BUCKET_NAME = 'wr-gooddata-test';
	const BUCKET_ID = 'sys.c-wr-gooddata-test';
	const WRITER_ID = 'test';

	/**
	 * @var \Keboola\StorageApi\Client
	 */
	protected static $storageApi;
	/**
	 * @var \Keboola\GoodDataWriter\GoodData\RestApi
	 */
	protected static $restApi;
	/**
	 * @var \Symfony\Bundle\FrameworkBundle\Client
	 */
	protected static $client;
	/**
	 * @var CommandTester
	 */
	protected static $commandTester;

	public static function setUpBeforeClass()
	{
		self::$client = static::createClient();
		self::$client->setServerParameters(array(
			'HTTP_X-StorageApi-Token' => self::$client->getContainer()->getParameter('storageApi.test.token')
		));

		self::$storageApi = new \Keboola\StorageApi\Client(self::$client->getContainer()->getParameter('storageApi.test.token'),
			self::$client->getContainer()->getParameter('storageApi.url'));
		self::$restApi = new \Keboola\GoodDataWriter\GoodData\RestApi(null, self::$client->getContainer()->get('logger'));

		// Clear test environment
		if (self::$storageApi->bucketExists(self::BUCKET_ID)) {
			$bucketInfo = self::$storageApi->getBucket(self::BUCKET_ID);
			foreach ($bucketInfo['tables'] as $table) {
				self::$storageApi->dropTable($table['id']);
			}
			self::$storageApi->dropBucket(self::BUCKET_ID);
		}

		// Init job processing
		$application = new Application(self::$client->getKernel());
		$application->add(new RunJobCommand());
		$command = $application->find('gooddata-writer:run-job');
		self::$commandTester = new CommandTester($command);
	}

	public static function tearDownAfterClass()
	{

	}

	public function testCreateWriter()
	{
		// Create and process job
		$this->_processJob('/gooddata-writer/writers', array());

		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			$_SERVER['KERNEL_DIR'] . '/tmp');

		// Check result
		$validConfiguration = true;
		try {
			$configuration->checkGoodDataSetup();
		} catch (\Keboola\GoodDataWriter\Exception\WrongConfigurationException $e) {
			$validConfiguration = false;
		}
		$this->assertTrue($validConfiguration);

		self::$restApi->login($configuration->bucketInfo['gd']['username'], $configuration->bucketInfo['gd']['password']);
		$projectInfo = self::$restApi->getProject($configuration->bucketInfo['gd']['pid']);
		$this->assertArrayHasKey('project', $projectInfo);
		$this->assertArrayHasKey('content', $projectInfo['project']);
		$this->assertArrayHasKey('state', $projectInfo['project']['content']);
		$this->assertEquals('ENABLED', $projectInfo['project']['content']['state']);

		$userInfo = self::$restApi->get($configuration->bucketInfo['gd']['userUri']);
		$this->assertArrayHasKey('accountSetting', $userInfo);

		$userProjectsInfo = self::$restApi->get($configuration->bucketInfo['gd']['userUri'] . '/projects');
		$this->assertArrayHasKey('projects', $userProjectsInfo);
		$this->assertCount(1, $userProjectsInfo['projects']);
		$projectFound = false;
		foreach ($userProjectsInfo['projects'] as $p) {
			if (isset($p['project']['links']['metadata']) && strpos($p['project']['links']['metadata'], $configuration->bucketInfo['gd']['pid']) !== false) {
				$projectFound = true;
				break;
			}
		}
		$this->assertTrue($projectFound);
	}

	public function testGetWriters()
	{
		self::$client->request('GET', '/gooddata-writer/writers');
		$response = self::$client->getResponse();
		$this->assertEquals($response->getStatusCode(), 200);
		$responseJson = json_decode($response->getContent(), true);
		$this->assertNotEmpty($responseJson);
		$this->assertArrayHasKey('writers', $responseJson);
		$this->assertCount(1, $responseJson['writers']);
		$this->assertEquals(self::WRITER_ID, $responseJson['writers'][0]['id']);
	}

	public function testCreateProject()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			$_SERVER['KERNEL_DIR'] . '/tmp');

		// Create and process job
		$this->_processJob('/gooddata-writer/projects', array());

		// Check result
		$projectsList = $configuration->getProjects();
		$this->assertCount(2, $projectsList);

		$project = $projectsList[1];
		self::$restApi->login($configuration->bucketInfo['gd']['username'], $configuration->bucketInfo['gd']['password']);
		$projectInfo = self::$restApi->getProject($project['pid']);
		$this->assertArrayHasKey('project', $projectInfo);
		$this->assertArrayHasKey('content', $projectInfo['project']);
		$this->assertArrayHasKey('state', $projectInfo['project']['content']);
		$this->assertEquals('ENABLED', $projectInfo['project']['content']['state']);
	}

	public function testGetProjects()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			$_SERVER['KERNEL_DIR'] . '/tmp');

		$projectsList = $configuration->getProjects();
		$project = $projectsList[1];

		self::$client->request('GET', '/gooddata-writer/projects?writerId=' . self::WRITER_ID);
		$response = self::$client->getResponse();
		$this->assertEquals($response->getStatusCode(), 200);
		$responseJson = json_decode($response->getContent(), true);
		$this->assertNotEmpty($responseJson);
		$this->assertArrayHasKey('projects', $responseJson);
		$this->assertCount(2, $responseJson['projects']);
		$this->assertEquals($project['pid'], $responseJson['projects'][1]['pid']);
	}

	public function testCreateUser()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			$_SERVER['KERNEL_DIR'] . '/tmp');

		// Create and process job
		$this->_processJob('/gooddata-writer/users', array(
			'email' => 'test' . time() . uniqid() . '@test.keboola.com',
			'password' => md5(uniqid()),
			'firstName' => 'Test',
			'lastName' => 'KBC'
		));

		// Check result
		$usersList = $configuration->getUsers();
		$this->assertCount(2, $usersList);

		$user = $usersList[1];
		$userInfo = self::$restApi->get($user['uri']);
		$this->assertArrayHasKey('accountSetting', $userInfo);
	}

	public function testGetUsers()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			$_SERVER['KERNEL_DIR'] . '/tmp');

		$usersList = $configuration->getUsers();
		$user = $usersList[1];

		self::$client->request('GET', '/gooddata-writer/users?writerId=' . self::WRITER_ID);
		$response = self::$client->getResponse();
		$this->assertEquals($response->getStatusCode(), 200);
		$responseJson = json_decode($response->getContent(), true);
		$this->assertNotEmpty($responseJson);
		$this->assertArrayHasKey('users', $responseJson);
		$this->assertCount(2, $responseJson['users']);
		$this->assertEquals($user['uri'], $responseJson['users'][1]['uri']);
	}

	public function testAddUserToProject()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			$_SERVER['KERNEL_DIR'] . '/tmp');

		$usersList = $configuration->getUsers();
		$user = $usersList[1];

		$projectsList = $configuration->getProjects();
		$project = $projectsList[1];

		// Create and process job
		$this->_processJob('/gooddata-writer/project-users', array(
			'email' => $user['email'],
			'pid' => $project['pid'],
			'role' => 'editor'
		));

		// Check result
		$userProjectsInfo = self::$restApi->get('/gdc/projects/' . $project['pid'] . '/users');
		$this->assertArrayHasKey('users', $userProjectsInfo);
		$this->assertCount(3, $userProjectsInfo['users']);
		$userInProject = false;
		foreach ($userProjectsInfo['users'] as $p) {
			if (isset($p['user']['content']['email']) && $p['user']['content']['email'] == $user['email']) {
				$userInProject = true;
				break;
			}
		}
		$this->assertTrue($userInProject);
	}

	public function testInviteUserToProject()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			$_SERVER['KERNEL_DIR'] . '/tmp');

		$usersList = $configuration->getUsers();
		$user = $usersList[1];

		$projectsList = $configuration->getProjects();
		$project = $projectsList[0];

		// Create and process job
		$this->_processJob('/gooddata-writer/project-invitations', array(
			'email' => $user['email'],
			'pid' => $project['pid'],
			'role' => 'editor'
		));

		// Check result
		$userProjectsInfo = self::$restApi->get('/gdc/projects/' . $project['pid'] . '/invitations');
		$this->assertArrayHasKey('invitations', $userProjectsInfo);
		$this->assertCount(3, $userProjectsInfo['invitations']);
		$userInProject = false;
		foreach ($userProjectsInfo['invitations'] as $p) {
			if (isset($p['invitation']['content']['email']) && $p['invitation']['content']['email'] == $user['email']) {
				$userInProject = true;
				break;
			}
		}
		$this->assertTrue($userInProject);
	}

	public function testGetProjectUsers()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			$_SERVER['KERNEL_DIR'] . '/tmp');

		$usersList = $configuration->getUsers();
		$user = $usersList[1];

		$projectsList = $configuration->getProjects();
		$project = $projectsList[1];

		self::$client->request('GET', '/gooddata-writer/project-users?writerId=' . self::WRITER_ID . '&pid=' . $project['pid']);
		$response = self::$client->getResponse();
		$this->assertEquals($response->getStatusCode(), 200);
		$responseJson = json_decode($response->getContent(), true);
		$this->assertNotEmpty($responseJson);
		$this->assertArrayHasKey('users', $responseJson);
		$this->assertCount(1, $responseJson['users']);
		$this->assertEquals($user['email'], $responseJson['users'][0]['email']);
	}

	public function testDeleteWriter()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			$_SERVER['KERNEL_DIR'] . '/tmp');

		// Create and process job
		$this->_processJob('/gooddata-writer/delete-writers', array());

		// Check result
		$this->assertFalse($configuration->configurationBucket(self::WRITER_ID));
	}


	/**
	 * Call API and process the job immediately
	 * @param $url
	 * @param $params
	 */
	protected function _processJob($url, $params)
	{
		self::$client->request('POST', $url, array(), array(), array(),
			json_encode(array_merge($params, array(
				'writerId' => self::WRITER_ID,
				'dev' => 1
			))));
		$response = self::$client->getResponse();

		$this->assertEquals($response->getStatusCode(), 200);
		$responseJson = json_decode($response->getContent(), true);
		$this->assertNotEmpty($responseJson);
		$this->assertArrayHasKey('job', $responseJson);

		self::$commandTester->execute(array(
			'command' => 'gooddata-writer:run-job',
			'job' => $responseJson['job']
		));
	}

}
