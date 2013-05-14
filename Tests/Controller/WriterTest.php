<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
use Symfony\Bundle\FrameworkBundle\Test\WebTestCase;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Bundle\FrameworkBundle\Client;
use Symfony\Component\Console\Tester\CommandTester;
use Keboola\GoodDataWriter\Command\RunJobCommand,
	Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\StorageApi\Table as StorageApiTable;

class WriterTest extends WebTestCase
{
	const BUCKET_NAME = 'wr-gooddata-test';
	const BUCKET_ID = 'sys.c-wr-gooddata-test';
	const DATA_BUCKET_ID = 'out.c-gdwtest';
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
	protected static $mainConfig;

	public static function setUpBeforeClass()
	{
		self::$client = static::createClient();
		$container = self::$client->getContainer();
		self::$client->setServerParameters(array(
			'HTTP_X-StorageApi-Token' => $container->getParameter('storageApi.test.token')
		));

		self::$mainConfig = $container->getParameter('gooddata_writer');
		self::$storageApi = new \Keboola\StorageApi\Client($container->getParameter('storageApi.test.token'),
			self::$client->getContainer()->getParameter('storageApi.url'));
		self::$restApi = new \Keboola\GoodDataWriter\GoodData\RestApi(null, $container->get('logger'));

		// Clear test environment
		if (self::$storageApi->bucketExists(self::BUCKET_ID)) {
			$bucketInfo = self::$storageApi->getBucket(self::BUCKET_ID);
			foreach ($bucketInfo['tables'] as $table) {
				self::$storageApi->dropTable($table['id']);
			}
			self::$storageApi->dropBucket(self::BUCKET_ID);
		}
		if (self::$storageApi->bucketExists(self::DATA_BUCKET_ID)) {
			$bucketInfo = self::$storageApi->getBucket(self::DATA_BUCKET_ID);
			foreach ($bucketInfo['tables'] as $table) {
				self::$storageApi->dropTable($table['id']);
			}
			self::$storageApi->dropBucket(self::DATA_BUCKET_ID);
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

		$userInfo = self::$restApi->getUser($configuration->bucketInfo['gd']['uid']);
		$this->assertArrayHasKey('accountSetting', $userInfo);

		$userProjectsInfo = self::$restApi->get(sprintf('/gdc/account/profile/%s/projects', $configuration->bucketInfo['gd']['uid']));
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

	public function testUploadProject()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			$_SERVER['KERNEL_DIR'] . '/tmp');

		// Prepare data
		self::$storageApi->createBucket('gdwtest', 'out', 'Writer Test');

		$table = new StorageApiTable(self::$storageApi, self::DATA_BUCKET_ID . '.categories', null, 'id');
		$table->setHeader(array('id', 'name'));
		$table->setFromArray(array(
			array('c1', 'Category 1'),
			array('c2', 'Category 2')
		));
		$table->save();

		$table = new StorageApiTable(self::$storageApi, self::DATA_BUCKET_ID . '.products', null, 'id');
		$table->setHeader(array('id', 'name', 'price', 'date', 'category'));
		$table->setFromArray(array(
			array('p1', 'Product 1', '45', '2013-01-01', 'c1'),
			array('p2', 'Product 2', '26', '2013-01-03', 'c2'),
			array('p3', 'Product 3', '112', '2013-01-03', 'c1')
		));
		$table->save();

		// Prepare Writer configuration
		$configuration->addDateDimension('ProductDate', false);

		$table = new StorageApiTable(self::$storageApi, 'sys.c-wr-gooddata-test.c-gdwtest_categories', null, 'name');
		$table->setAttribute('tableId', self::DATA_BUCKET_ID . '.categories');
		$table->setAttribute('gdName', 'Categories');
		$table->setAttribute('export', '1');
		$table->setHeader(array('name', 'gdName', 'type', 'dataType', 'dataTypeSize', 'schemaReference', 'reference',
			'format', 'dateDimension', 'sortLabel', 'sortOrder'));
		$table->setFromArray(array(
			array('id', 'Id', 'CONNECTION_POINT', '', '', '', '', '', '', '', ''),
			array('name', 'Name', 'ATTRIBUTE', '', '', '', '', '', '', '', '')
		));
		$table->save();

		$table = new StorageApiTable(self::$storageApi, 'sys.c-wr-gooddata-test.c-gdwtest_products', null, 'name');
		$table->setAttribute('tableId', self::DATA_BUCKET_ID . '.products');
		$table->setAttribute('gdName', 'Products');
		$table->setAttribute('export', '1');
		$table->setHeader(array('name', 'gdName', 'type', 'dataType', 'dataTypeSize', 'schemaReference', 'reference',
			'format', 'dateDimension', 'sortLabel', 'sortOrder'));
		$table->setFromArray(array(
			array('id', 'Id', 'CONNECTION_POINT', '', '', '', '', '', '', '', ''),
			array('name', 'Name', 'ATTRIBUTE', '', '', '', '', '', '', '', ''),
			array('price', 'Price', 'FACT', '', '', '', '', '', '', '', ''),
			array('date', 'Date', 'DATE', '', '', '', '', 'yyyy-MM-dd', 'ProductDate', '', ''),
			array('category', 'Category', 'REFERENCE', '', '', self::DATA_BUCKET_ID . '.categories', '', '', '', '', '')
		));
		$table->save();

		// Create and process job
		$this->_processJob('/gooddata-writer/upload-project', array());


		$datasetsData = self::$restApi->get('/gdc/md/' . $configuration->bucketInfo['gd']['pid'] . '/data/sets');
		$this->assertArrayHasKey('dataSetsInfo', $datasetsData);
		$this->assertArrayHasKey('sets', $datasetsData['dataSetsInfo']);
		$this->assertCount(3, $datasetsData['dataSetsInfo']['sets']);

	}

	public function testUploadTable()
	{
		//@TODO
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
		$userInfo = self::$restApi->getUser($user['uid']);
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
		$this->assertEquals($user['uid'], $responseJson['users'][1]['uid']);
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
		$this->assertCount(1, $userProjectsInfo['invitations']);
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

	public function testCreateFilter()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			$_SERVER['KERNEL_DIR'] . '/tmp');

		// Create and process job
		$this->_processJob('/gooddata-writer/filters', array(
			"name"      => "filter",
			"attribute" => "name (Users)",
			"element"   => "miro"
		));

		// Check result
		$filterList = $configuration->getFilters();
		$this->assertCount(2, count($filterList));
	}

	public function testAssignFilterToUser()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			$_SERVER['KERNEL_DIR'] . '/tmp');

		$usersList = $configuration->getUsers();
		$user = $usersList[1];

		// Create and process job
		$this->_processJob('/gooddata-writer/filters-user', array(
			"name"      => "filter",
			"attribute" => "name (Users)",
			"element"   => "miro"
		));
	}

	public function testDeleteFilter()
	{

	}

	public function testSyncFilter()
	{

	}

	public function testDeleteWriter()
	{
		$configuration = new \Keboola\GoodDataWriter\Writer\Configuration(self::WRITER_ID, self::$storageApi,
			$_SERVER['KERNEL_DIR'] . '/tmp');

		self::$restApi->login(self::$mainConfig['gd']['dev']['username'], self::$mainConfig['gd']['dev']['password']);

		foreach ($configuration->getProjects() as $p) {
			self::$restApi->dropProject($p['pid']);
		}
		foreach ($configuration->getUsers() as $u) {
			self::$restApi->dropUser($u['uid']);
		}


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

		if (isset($responseJson['job'])) {
			self::$commandTester->execute(array(
				'command' => 'gooddata-writer:run-job',
				'job' => $responseJson['job']
			));
		} else if (isset($responseJson['batch'])) {
			self::$client->request('GET', sprintf('/gooddata-writer/batch?writerId=%s&id=%d', self::WRITER_ID, $responseJson['batch']));
			$response = self::$client->getResponse();
			$responseJson = json_decode($response->getContent(), true);
			$this->assertArrayHasKey('batch', $responseJson);
			$this->assertArrayHasKey('jobs', $responseJson['batch']);
			foreach ($responseJson['batch']['jobs'] as $job) {
				self::$commandTester->execute(array(
					'command' => 'gooddata-writer:run-job',
					'job' => $job
				));
			}
		} else {
			$this->assertTrue(false);
		}
	}

}
